const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const pubsub   = require( '../cluster-mgr/pubsub' )

const dbDocCre  = require( './db-doc-cre' )
const dbDocFind = require( './db-doc-find' )
const dbDocUpd  = require( './db-doc-upd' )
const dbDocDel  = require( './db-doc-del' )
const dbMetrics = require( './db-metrics' )
const persistence = require( './db-persistence' )

let nodeMgr = null

module.exports = {
  init,
  terminate,
  getJobs,
  checkJobToDos,
  sendDataBatch,
  storeDataBatch,
  checkForJob,
  manageJobs,
  creTransferDataJobs
}

let cfg = {
  MODE : 'RMQ',
  DATA_REPLICATION : 3,
  TOKEN_LEN : 1
}


async function init( configParams, nodeMgrObj ) {
  log.info( 'Init DB-Jobs ...')
  cfgHlp.setConfig( cfg, configParams )
  nodeMgr = nodeMgrObj 
}


async function terminate() {
  log.info( 'Terminate DB-Jobs ...' )
  // TODO
}


// ============================================================================

async function getJobs() {
  let inLeadJobs = []
  let passiveJobs = []
  let jobsResult = await dbDocFind.find( 'admin', 'job', { } )
  if ( ! jobsResult._error && jobsResult.data ) { 
    for ( let job of jobsResult.data ) {
      if ( job.nodeId == nodeMgr.ownNodeId() ) {
        inLeadJobs.push( job )
      } else 
      if ( job.toNode == nodeMgr.ownNodeId() ) {
        passiveJobs.push( job )
      }
    }
    inLeadJobs.sort( (a,b) => { return ( a._cre - b._cre ) })
    passiveJobs.sort( (a,b) => { return ( a._cre - b._cre ) })
  }
  return { 
    inLeadJobs: inLeadJobs,
    passiveJobs: passiveJobs
  }
}

async function manageJobs( jobs ) {
  if ( ! jobs || jobs.length == 0 ) { return }
  let job = jobs[ 0 ]
  // log.info( 'JOOOOb', JSON.stringify(job))
  if ( ! job.started ) {
    await startBatch( job )
  } else {
    if ( job.sentNextBatch ) {
      await sendDataBatch( job )
    }
  }
  // let cre1 = jobsResult.data [0]._cre
  // for ( let job of jobs ) {
  //   log.info( 'jobs', job._cre, cre1 - job._cre, job.jobId, job.done, job.started )
  // }
}


async function checkJobToDos( jobs ) {
  if ( ! jobs || jobs.length == 0 ) { return }
  for ( let job of jobs ) {
    if ( job.started ) {
      log.info( job.jobId, 'CHECK CLIENT JOBS started ... ' )
      if ( job.waitingForReceiver ) {
        await requestNextBatch( job )
      } else {
        log.info( job.jobId, 'CHECK CLIENT JOBS',  Date.now() - job._chg,  Date.now() , job._chg)
        if ( Date.now() - job._chg > 60000 ) {
          await requestNextBatch( job )
        }
      }  
    } 
  }
}


const BATCH_COUNT = 10


async function sendDataBatch( job ) {
  log.info( job.jobId, 'jobs','######### sendDataBatch', job.db, job.coll, job.fromNode, job.token, job.done )
  
  let batchIds = await persistence.getAllDocIds( job.jobId, job.db, job.coll, { start: job.done, count: BATCH_COUNT } )
  if ( idArr._error ) { return batchIds }
  for ( let docId of batchIds ) {

    let doc = await persistence.getDocById( job.db, job.coll, docId )
    let dataMsg = {
      _id      : job._id,
      jobId    : job.jobId,
      action   : 'InsertLocally',
      fromNode : job.rmFrmNode,
      toNode   : job.toNode,
      db       : job.db, 
      coll     : job.coll,
      data     : [ doc ] // TODO, check size and send many
    }
    log.info( job.jobId, 'jobs','######### sendDataBatch DOC', job.db, job.coll, docId )

    await pubsub.sendToQueue( job.jobId, job.queue, 'TransferData', dataMsg )
  }

  let lastBatch = false
  if ( batchIds.length < BATCH_COUNT ) { 
    lastBatch = true
  }

  let batchEndMsg = {
    _id      : job._id,
    jobId    : job.jobId,
    action   : ( lastBatch ? 'Completed' : 'BatchEnd' ),
    fromNode : job.rmFrmNode,
    toNode   : job.toNode,
    db       : job.db, 
    coll     : job.coll,
  }
  await pubsub.sendToQueue( job.jobId, job.queue, 'TransferData', batchEndMsg )
}

async function storeDataBatch( job ) {
  log.info( 'storeDataBatch', job  )
  switch ( job.action  ) {

    case 'InsertLocally' :
      for ( let doc in job.data ) {
        persistence.insertDocPrep( job.jobId, job.db, job.coll, doc ) 
      }
      break

    case 'BatchEnd' :
      await requestNextBatch( job, BATCH_COUNT )
      break

    case 'Completed' :
      await endJob( job )
      break

    default:
      break;
  }
}


async function startBatch( job ) {
  log.info( job.jobId, 'jobs','######### startBatch', job.db, job.coll, job.fromNode+'>'+job.toNode, job.token )
  await updateJob( job, { 
    started            : true, 
    waitingForReceiver : true, 
    sentNextBatch      : false,
    batchDone          : false
  })
}

async function requestNextBatch( job, done = 0 ) {
  log.info( job.jobId, 'jobs','######### requestNextBatch', job.db, job.coll, job.fromNode+'>'+job.toNode, job.done + BATCH_COUNT,  pubsub.getReplyQueue() )
  await updateJob( job, { 
    sentNextBatch      : true,
    batchDone          : true,
    waitingForReceiver : false,
    done               : job.done + done,
    queue              : pubsub.getReplyQueue()
  })
}

async function updateJob( job, update ) {
  log.info( job.jobId, 'jobs','######### send updateJob', job.db, job.coll, job.fromNode+'>'+job.toNode, job.token )
  const JOB_UPD = { db : 'admin', coll: 'job', txnId : job.jobId, 
    update: { $set: update } 
  }
  await updateOneDoc( JOB_UPD, { _id: job._id }, { allNodes: true } )
}

async function updateOneDoc( r, doc, opt = {} ) {
  if ( ! doc._id ) { return { _error: 'Require _id' } }
  let docbyId = await persistence.getDocById( r.db, r.coll,  doc._id )
  if ( docbyId._error ) { return docbyId }
  let result = await dbDocUpd.updateOneDoc( r, doc, docbyId.doc )
  dbMetrics.addDbMetric( r.db, r.coll, "upd", result )
  return result
}

async function endJob( job ) {
  log.info( job.jobId, 'jobs','######### send endJob', job )
  const JOB = { db : 'admin', coll: 'job', txnId : job.jobId+'.DEL' }
  await dbDocDel.deleteOneDocAllNodes( JOB, job._id)
}


async function checkForJob( dta ) {
  if ( dta.db == 'admin' && dta.col == 'job' ) {
    let job = await persistence.getDocById( dta.db, dta.col, dta.docId )
    //log.info('CHECK JOB >>>>>>>>>>>>>>>>>>>>>>>>>>>>',  dta.upd,  job.doc?.fromNode, nodeMgr.ownNodeId() )
    if ( job.doc?.fromNode == nodeMgr.ownNodeId() ) {
      manageJobs([ job.doc ])
    } else  
    if ( job.doc?.toNode == nodeMgr.ownNodeId() ) {
      checkJobToDos([ job.doc ])
    }
  }
}

// ============================================================================

async function creTransferDataJobs( task ) {
  log.info( 'TransferTokenData >>>', task )
  const JOB_COLL = { db : 'admin', coll: 'job', txnId : task.jobId }
  let subTsk = 0
  let dbTree = await getDbTree()
  // log.info( 'TransferTokenData', dbTree )
  for ( let dbName in dbTree ) {
    log.info( 'TransferTokenData', dbName )
    for ( let collName in dbTree[ dbName ].c ) {
      if ( dbName == 'admin' && collName == 'job' ) { continue }
      let coll = dbTree[ dbName ].c[ collName ]
      log.info( 'TransferTokenData', dbName, collName, coll.masterData  )

      let transferJob = {
        job      : 'TransferTokenData',
        action   : task.action,
        jobId    : task.jobId +'.'+  nodeMgr.ownNodeId() +'.'+ subTsk,
        nodeId   : nodeMgr.ownNodeId(),
        fromNode : task.fromNode, // thats me
        toNode   : task.toNode,
        db       : dbName,
        coll     : collName,
        done     : 0
      }

      if ( coll.masterData ) {
        if ( task.action == 'CopyMasterData' ) { 
          transferJob.token  = '*'
        } else { continue }
      } else {
        transferJob.token  = task.token
      }

      if ( task.master ) {
        transferJob. master = task.masterNode,
        transferJob.replica = task.replicaNode
      }

      log.info( 'TransferTokenData', JSON.stringify( transferJob ) )
      await dbDocCre.insertOneDoc( JOB_COLL, transferJob )
      subTsk ++
      
    }
  }
}