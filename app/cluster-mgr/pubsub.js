const log  = require( '../helper/logger' ).log
const amqp = require( 'amqplib' )
const hlp  = require( './node-mgr-helper' )
const cfgHlp = require( '../helper/config' )

module.exports = {
  init,
  
  subscribeJobs,
  sendDbOp,
  sendBackupOp,
  sendJob,
  sendToQueue,
  
  subscribeDataUpdates,
  subscribeDataBroadcasts,

  sendRequest,
  sendRequestAllNodes,
  sendResponse,
  getReplies,

  getReplyQueue,

  terminate
}


let connection  = null
let channel     = null
let dataQueue   = null
let jobQueue    = null
let replyQueue  = null

let NODE_NAME = null
let QUORUM = 2

let incMetric = () => {}


const Q_MSG_IN  = 5
const Q_MSG_OUT = 6
const Q_JOB_IN  = 7
const Q_JOB_OUT = 8

let cfg = {
  MODE       : 'RMQ',
  RMQ_URL    : 'amqp://localhost',  // not used if cfg.MODE == "SINGLE_NODE"
  RMQ_PREFIX : 'DB_',
  RMQ_JOB_EXCHANGE : 'DB_node_jobs'
}

async function init( nodeName, configParams, incMetricFunc, jobDispatcher, dbQuorum = 2  ) {
  NODE_NAME = nodeName
  cfgHlp.setConfig( cfg, configParams )

  incMetric = incMetricFunc
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: SKIP RMQ init' )
    QUORUM = 1
    return 
  }
  log.info( 'RMQ: Init ...' )
  QUORUM    = dbQuorum
  try {
    connection = await amqp.connect( cfg.RMQ_URL )
    channel    = await connection.createChannel()

    process.once( 'SIGINT', async () => { 
      await channel.close()
      await connection.close()
      log.info( 'RMQ Subscription', 'Channel and connection closed.' )
    })

    await initTokenExchanges()
    await iniJobExchanges()
    await initLogExchanges()

    let queueBaseName = cfg.RMQ_PREFIX + NODE_NAME.replaceAll( ':', '_' ).replaceAll( '.', '_' ).replaceAll( '/', '_' )
    dataQueue = await initDataQueue( queueBaseName )
    jobQueue  = await initJobQueue(  queueBaseName )
    logQueue  = await initLogQueue(  queueBaseName )
   
    replyQueue = await initReplyProcessor( jobDispatcher )

  } catch (err) { 
    log.error( 'RMQ init', err ) 
    process.exit()
  }
}

let consumerTags = []

async function terminate() {
  if ( ! channel ) { return }
  try {
    for ( let consumer of consumerTags ) {
      log.info( 'Terminate DataQueue', consumer )
      channel.cancel( consumer )  
    }
    log.info( 'Terminate JobQueue...' )
    channel.cancel( 'JobQueue' )
  } catch ( exc ) { log.info( 'RMQ terminate', exc ) }
  // // try {
  // //   log.info( 'Terminate messaging: Close channel...' )
  // //   await channel.close()
  // // } catch ( exc ) { log.info( 'RMQ terminate', exc ) }
  // try {
  //   log.info( 'Terminate messaging: Close connection...')
  //   await connection.close()
  //   // 
  // } catch ( exc ) { log.info( 'RMQ terminate', exc ) }
}


function getReplyQueue() {
  return replyQueue
}

// ============================================================================
// JOBS

async function iniJobExchanges() { 
  await channel.assertExchange( cfg.RMQ_JOB_EXCHANGE , 'topic', { durable: true } )
}

async function initJobQueue( nodeName ) {
  log.info( 'RMQ: Init Job Queue ...')
  const { queue } = await channel.assertQueue( nodeName+'_Job', { durable: true } )
  return queue
}

let jobCallbackFct = () => {}

async function subscribeJobs( jobCallback ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: SKIP subscribeJobs' )
    jobCallbackFct = jobCallback
    return 
  }

  try {
    log.info( 'RMQ subscribeJobs', cfg.RMQ_JOB_EXCHANGE )

    await channel.bindQueue( jobQueue, cfg.RMQ_JOB_EXCHANGE, '' )

    channel.consume( jobQueue, async ( message ) => {
      if ( message ) {
        let msgHdr = message.properties.headers

        if ( msgHdr.jobType != 'SyncNodes' ) {

          let job = {
            from      : msgHdr.from,
            jobType   : msgHdr.jobType,
            jobId     : msgHdr.jobId,
            timestamp : message.properties.timestamp,
            task      : JSON.parse( message.content.toString() )
          }
          log.debug( job.jobId, 'RMQ job', job.task.op )
          await jobCallback( job, message.properties )
          incMetric( Q_JOB_IN )

        } else if ( msgHdr.from != NODE_NAME ) { // "SyncNodes" task from another node

          let job = {
            from      : msgHdr.from,
            jobType   : msgHdr.jobType,
            timestamp : message.properties.timestamp,
            task      : JSON.parse( message.content.toString() )
          }
          await jobCallback( job, message.properties )
          incMetric( Q_JOB_IN )

        } else {
          log.debug( 'RMQ job', 'ignore "SyncNodes" from myself')
        }
      } else {
        console.warn('RMQ job consumer cancelled: Stop process!')
        process.exit()
      }
    }, { noAck: true, consumerTag: 'JobQueue' } )

  } catch (err) { 
    log.error( 'RMQ subscribeJobs', err ) 
    process.exit()
  }
}

async function sendDbOp( jobId, op, params, needReply= false ) {
  log.debug( jobId, '>>> RMQ send DB op', op, params )
  let task = params
  task.op    = op
  task.jobId = jobId
  await sendJob( 'DB Op', task, true, needReply )
}

async function sendBackupOp( jobId, op, params, needReply= false ) {
  log.debug( jobId, '>>> RMQ send DB op', op, params )
  let task = params
  task.op    = op
  task.jobId = jobId
  await sendJob( 'Backup Op', task, true, needReply )
}

async function sendJob( type, task, persistent = true, needReply=false ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: job', type, task )
    let job = {
      from      : NODE_NAME,
      jobType   : type,
      timestamp : Date.now(),
      task      : task,
      jobId     : task.jobId
    }
    jobCallbackFct( job, {} ) // no await ??
    return 
  }

  log.debug( task.jobId, 'RMQ sendJob', type, task )
  try {
    let pubOpts = { 
      timestamp   : Date.now(), 
      contentType : 'application/json',
      persistent  : persistent,
      headers: {
        from    : NODE_NAME,
        jobType : type,
        jobId   : task.jobId
      }
    }
    if ( needReply ) {
      pubOpts.replyTo       = replyQueue 
      pubOpts.correlationId = task.jobId
    }
    channel.publish( cfg.RMQ_JOB_EXCHANGE, '', Buffer.from( JSON.stringify( task ) ), pubOpts )
    incMetric( Q_JOB_OUT )
  } catch (err) { 
    log.error( 'RMQ sendJobs', err ) 
    process.exit()
  }
}

// ============================================================================
// DATA

// create an exchange for every data token
async function initTokenExchanges() { 
  let allTokens = hlp.genTokens( )
  for( let token of allTokens ) {
    let exchange = cfg.RMQ_PREFIX + 'token_' + token
    await channel.assertExchange( exchange , 'topic', { durable: true } )
  }
  let exchange = cfg.RMQ_PREFIX + 'all_nodes'
  await channel.assertExchange( exchange , 'topic', { durable: true } )
}

async function initDataQueue( nodeName ) {
  log.debug( 'RMQ: Init Data Queue ...')
  const { queue } = await channel.assertQueue( nodeName+'_Data', { durable: true } )
  return queue
}

let dataUpdCallbackFct    = () => {}
let dataAllUpdCallbackFct = () => {} // should equal dataUpdCallbackFct, but ...

async function subscribeDataUpdates( token, dataUpdCallback ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    // log.debug( 'SINGLE_NODE: SKIP subscribeDataUpdates' )
    dataUpdCallbackFct = dataUpdCallback
    return 
  }

  let tokenExchange = cfg.RMQ_PREFIX + 'token_' + token.toLowerCase()
  subscribeExchDataUpdates( tokenExchange, dataUpdCallback )
}

async function subscribeDataBroadcasts( dataUpdCallback ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: SKIP subscribeDataBroadcasts' )
    dataAllUpdCallbackFct = dataUpdCallback
    return 
  }

  let allExchange = cfg.RMQ_PREFIX + 'all_nodes'
  subscribeExchDataUpdates( allExchange, dataUpdCallback )
}

let dataSubscriptions = []

async function subscribeExchDataUpdates( exchange, dataUpdCallback ) {
  try {
    if ( dataSubscriptions.includes( exchange ) ) { return } // already subscribed

    log.info( 'RMQ subscribeDataUpdates', exchange )
    dataSubscriptions.push( exchange )
    await channel.bindQueue( dataQueue, exchange, '' )

    channel.consume( dataQueue, async ( message ) => {
      if ( message ) {
        let msgHdr = message.properties.headers
        // if ( msgHdr.from != NODE_NAME ) {
          let opCmd = {
            token     : msgHdr.token,
            timestamp : message.properties.timestamp,
            msgProp   : message.properties,
            data      : JSON.parse( message.content.toString() )
          }
          if ( ! opCmd.data.txnId ){
            log.debug( '//// RMQ msg', message.content.toString() )
            log.debug( '//// RMQ msg', message )
          } else {
            log.debug( opCmd.data.txnId, 'RMQ msg', opCmd.data.op )
            await dataUpdCallback( opCmd )
           }
          incMetric( Q_MSG_IN )
        // } else {
        //   log.info( 'RabbitMQ consume', 'ignore msg from myself')
        // }
      } else {
        console.warn('RabbitMQ consumer cancelled: Stop process!')
        process.exit()
      }
    }, { noAck: true, consumerTag: exchange } )
    consumerTags.push( exchange )

  } catch (err) { 
    log.error( 'RMQ subscribeDataUpdates', err ) 
    process.exit()
  }
}

async function sendRequest( txnId, token, reqData ) {
  let exchangeForToken = cfg.RMQ_PREFIX + 'token_' + token.toLowerCase()
  sendExchDataUpdate( exchangeForToken, txnId, token, reqData )
}

async function sendRequestAllNodes( txnId, reqData ) {
  let exchangeAllNodes = cfg.RMQ_PREFIX + 'all_nodes'
  sendExchDataUpdate( exchangeAllNodes, txnId, null, reqData )
}

async function sendExchDataUpdate( exchange, txnId, token, reqData ) {
  log.debug( txnId, 'RMQ publish...', exchange, reqData )
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: sendExchDataUpdate', txnId, token, reqData )
    // if ( msgHdr.jobType != 'SyncNodes' ) { return }
    let opCmd = {
        token     : token,
        timestamp : Date.now(),
        msgProp   : { correlationId: reqData.txnId, headers: {} },
        data      : reqData  // contains also txnId
      }
    if ( token ) {
      dataUpdCallbackFct( opCmd ) // no await ??
    } else {
      dataAllUpdCallbackFct( opCmd )
    }
    return 
  }

  try {
    log.debug( txnId, 'RMQ publish', exchange, replyQueue )
    channel.publish( exchange, '', Buffer.from( JSON.stringify( reqData ) ),
      { timestamp     : Date.now(), 
        contentType   : 'application/json',
        persistent    : true,
        correlationId : txnId,
        replyTo       : replyQueue,
        headers: {
          from  : NODE_NAME,
          token : token
        }
      }
    )
    incMetric( Q_MSG_OUT )
  } catch (err) { 
    log.error( txnId, 'RMQ sendExchDataUpdate', err ) 
    process.exit()
  }
}

// ----------------------------------------------------------------------------
const REPLY_TIMEOUT = 5000

async function sendResponse( txnId, result ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: SKIP sendResponse', txnId, result )
    replyMsg[ txnId ] = {
      creDt : Date.now(),
      msg : [ result.data ]
    }
    if ( result.msgProp.headers?.resultIsArray ) {
      replyMsg[ txnId ].resultIsArray = true
    }
    return 
  }

  log.debug(  result.msgProp.correlationId, 'RMQ send response' )
  let headers = { from : NODE_NAME }
  if ( result.msgProp.headers?.resultIsArray ) {
    headers.resultIsArray = true
  }
  channel.sendToQueue(
    result.msgProp.replyTo,
    Buffer.from( JSON.stringify( result.data ) ),
    {
      correlationId: result.msgProp.correlationId,
      expiration : REPLY_TIMEOUT * 2,
      headers    : headers
    }
  )
}

// Send data to a specific node:
async function sendToQueue( txnId, queue, jobType,  data  ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: sendToQueue', type, task.op )
    return // only used for data transfer between nodes
  }

  log.info( txnId, 'sendToQueue', jobType, queue )
  try {
    let msgOpts = {
      correlationId : txnId,
      expiration    : REPLY_TIMEOUT * 20,
      headers       : { 
        jobType : jobType,
        from    : NODE_NAME,
        direct  : true 
      }
    }
    log.info( txnId, 'sendToQueue', msgOpts )
    channel.sendToQueue( queue, Buffer.from( JSON.stringify( data ) ), msgOpts )  
  } catch (error) {
    log.error( txnId, 'sendToQueue', jobType, queue, exc )
  }
}


let replyMsg = {}
let lastReplyMsg = Date.now()

let jobDispatcherFct =  () => {}

async function initReplyProcessor( jobDispatcher ) {
  if ( cfg.MODE == "SINGLE_NODE" ) { // don't need a message broker
    log.debug( 'SINGLE_NODE: SKIP initReplyProcessor' )
    jobDispatcherFct = jobDispatcher
    return 
  }

  const { queue } = await channel.assertQueue( '', { durable: false,  exclusive: true } )
  await channel.consume( queue, async ( message ) => {
    try {
      if ( ! message ) {
        log.warn( 'RabbitMQ consumer cancelled: Stop process!' )
        process.exit()
      }
      let txnId = message.properties.correlationId
      log.debug( txnId, 'RMQ reply', queue, message.properties.headers )
      log.debug( 'RMQ reply', txnId, message.properties.headers.from, message.content.toString() )

      if ( message.properties.headers.direct ) { // direct message to other node, eg data sync
        let job = {
          from      : message.properties.headers.from,
          jobType   : message.properties.headers.jobType,
          jobId     : message.properties.headers.jobId,
          timestamp : message.properties.timestamp,
          task      : JSON.parse( message.content.toString() )
        }
        await jobDispatcher( job )

      } else {

        if ( replyMsg[ txnId ] ) {
          replyMsg[ txnId ].msg.push( message )
        } else {
          replyMsg[ txnId ] = {
            creDt : Date.now(),
            msg : [ message ]
          }
        }
      
        if (  message.properties.headers.resultIsArray ) {
          replyMsg[ txnId ].resultIsArray = true
        }
        lastReplyMsg = Date.now()
      }
    } catch ( exc ) {
      log.error( 'ReplyProcessor', exc, message )
    }
  }, { noAck: true } )

  setInterval( cleanUpReplyMsg, 10000 )
  
  return queue
}

function cleanUpReplyMsg() {
  let retireDate = Date.now() - ( REPLY_TIMEOUT * 2 )
  for ( let txnId in replyMsg ) {
    if ( replyMsg[ txnId ].creDt > retireDate ) {
      log.debug( txnId, 'RMQ cleanup reply' )
      delete replyMsg[ txnId ]
    }
  }
}

async function getReplies( txnId, minReplyCnt = QUORUM ) {
  let startDate = Date.now()
  while ( true ) {
    // log.info( txnId, 'RMQ wait for replies', minReplyCnt )
    if ( replyMsg[ txnId ] && replyMsg[ txnId ].msg.length >= minReplyCnt ) {
      // for array queries the pod should return the tokens // if tokens are complete we can return the reply
      if ( txnId && ! txnId.startsWith('MX') )
        log.debug( txnId, 'RMQ return replies', replyMsg[ txnId ].msg.length )
      let result = { 
        _ok: true, 
        replyMsg: replyMsg[ txnId ].msg 
      } // TODO improve

      for ( let msg of replyMsg[ txnId ].msg ) {
        if ( msg._error ) { 
          result._ok    = false 
          result._error = msg._error
        }
      }

      if ( replyMsg[ txnId ].resultIsArray ) {
        result.resultIsArray = true
      }

      log.debug( txnId, 'REPLIES', result )
      return result
    }
    if ( Date.now() - startDate > REPLY_TIMEOUT ) {
      log.warn( txnId, 'RMQ wait for replies: TIMEOUT' ) 

      return { 
        _ok: false,
        _error: 'Timeout',
        replyMsg: replyMsg[ txnId ]?.msg, // return the results we have
        resultIsArray: replyMsg[ txnId ]?.resultIsArray
      }
    }
    await sleep( 10 )
  }
}

async function sleep ( ms ) {
  return new Promise( 
    resolve => setTimeout( resolve, ms )
  )
}

// ============================================================================
// LOGS

async function initLogExchanges() { 
  let exchange = cfg.RMQ_PREFIX + 'node_logs'
  await channel.assertExchange( exchange , 'topic', { durable: true } )
}


async function initLogQueue( nodeName ) {
  log.info( 'Init Job Queue ...')
  const { queue } = await channel.assertQueue( nodeName+'_Logs', { durable: true } )
  return queue
}

