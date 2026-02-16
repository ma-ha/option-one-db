const log     = require( '../helper/logger' ).log
const helper  = require( './db-helper' )
const pubsub  = require( '../cluster-mgr/pubsub' )

const persistence = require( './db-persistence' )

module.exports = {
  initAdminDB,
  
  processDbOp,

  creDB,
  getDB,
  listDBs,
  // getDbVersion,
  // updDbVersion,
  delDB,
  
  creColl,
  getColl,
  renameColl,
  getCollSpec,
  delColl,

  nodesUpdCollIdx,
  reIndexColl,
  listCollIdx,
  delCollIdx

}

const REQ_REPLY = true

// ============================================================================

async function initAdminDB( jobId, logExpireDays = 31 ) {
  let userTableEmpty = false
  log.info( jobId, 'initAdminDB, check/create collections ####################################################')
  await creDB( 'admin' ) 
  log.info( jobId, 'initAdminDB job')
  await creColl( jobId+'job', 'admin', 'job', { pk : ['jobId'], masterData : true } )

  log.info( jobId, 'initAdminDB monitoring')
  await creColl( jobId+'mon', 'admin', 'monitoring', { pk : ['_id'], masterData : true } )
  
  log.info( jobId, 'initAdminDB log')
  // if ( ! await getColl(  'admin', 'log' ) ) {
  await creColl( jobId+'log', 'admin', 'log', { pk : ['_id'] } )
  // }
  
  log.info( jobId, 'initAdminDB metrics')
  await creColl( jobId+'amtr', 'admin', 'api-metrics', { pk : ['podName'] } )
  await creColl( jobId+'dmtr', 'admin', 'db-metrics', { pk : ['db'] } )
  log.info( jobId, 'initAdminDB session')
  await creColl( jobId+'ses', 'admin', 'session', helper.adminDbSessionSpec() )
  log.info( jobId, 'initAdminDB audit-log')
  await creColl( jobId+'aud', 'admin', 'audit-log', { pk : ['sp','ts'] } )
  log.info( jobId, 'initAdminDB user')
  if ( ! await getColl(  'admin', 'user' ) ) {
    await creColl( jobId+'usr', 'admin', 'user', helper.adminDbUserSpec() )
    userTableEmpty = true
  } else {
    log.info( jobId, '"Coll admin.user" already initialized')
  }
  await creColl( jobId+'acc', 'admin', 'api-access', { pk : ['db'], masterData : true } )
  
  await creColl( jobId+'bac', 'admin', 'backup', { pk: ['_id'], masterData : true } )
  await creColl( jobId+'sch', 'admin', 'backup-schedule', { pk: ['_id'], masterData : true } )

  await nodesUpdCollIdx( jobId+'lidx', 'admin', 'log', 
    { t : { expiresAfterSeconds: logExpireDays * 24 * 60 * 60 } }
  )
  
  return userTableEmpty
}

// ============================================================================

async function processDbOp( job, msgProp ) {
  log.debug( job.jobId, 'DB process op', job.task.op )
  let opResult = null 

  let tsk = job.task
  switch ( job.task.op ) {

    case 'create database': 
      opResult = await persistence.creDBjob( job.jobId, job.task.dbName, { c: {} } )
      break
  
    case 'create collection':
      opResult = await persistence.creColl( job.jobId, tsk.dbName, tsk.collName, tsk.collOpts )
      break

    case 'update index':
      opResult = await persistence.updateIdx( job.jobId, tsk.dbName, tsk.collName, tsk.idx )
      break

    case 're-index':
      opResult = await persistence.reIdx( job.jobId, tsk.dbName, tsk.collName )
      break
    
    case 'drop index':
      opResult = await persistence.delCollIdx( job.jobId, tsk.dbName, tsk.collName, tsk.idxName )
      break

    case 'rename collection':
      opResult = await persistence.renameColl( job.jobId, tsk.dbName, tsk.oldCollName, tsk.newCollName )
      break

    case 'drop collection':
      opResult = await persistence.delColl( job.jobId, tsk.dbName, tsk.collName )
      break

    case 'drop database':  
      opResult = await persistence.delDbJob( job.jobId, tsk.dbName )
      break
  
    default: log.warn('UNKNOWN DB operation:', job.task.op ); break
  }

  log.debug( job.jobId, 'DB process op >>>> result', opResult )
  
  let result = {
    msgProp : msgProp,
    data    : opResult
  }
  await pubsub.sendResponse( job.jobId, result )
}

// ============================================================================

async function creDB( dbName, jobId ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 'create database', { dbName: dbName }, REQ_REPLY )
  let result = await pubsub.getReplies( jobId )
  return result 
}

async function delDB( dbName, jobId ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 'drop database', { dbName: dbName }, REQ_REPLY )
  let result = await pubsub.getReplies( jobId )
  return result
}


async function getDB( dbName ) { 
  return await persistence.getDB( dbName )
}

async function listDBs( ) {
  return await persistence.listDBs( )
}

// not used yet

// async function getDbVersion( dbName ) {
//   return await persistence.getDbVersion( dbName )
// }

// async function updDbVersion( dbName, version ) {
//   let result = await persistence.getDbVersion( dbName, version )
//   await nodeMgr.broadcastNodeUpdate()
//   return result
// }

// ----------------------------------------------------------------------------

async function creColl( jobId, dbName, collName, collOpts ) {
  if ( collName != 'api-metrics') { log.info( jobId, 'DB creColl', dbName, collName, collOpts ) }
  if ( ! collOpts || ! Array.isArray( collOpts.pk ) ) {
    return { _error: 'coll spec invalid' }
  }
   await pubsub.sendDbOp( jobId, 
    'create collection', 
    { 
      dbName   : dbName,
      collName : collName,
      collOpts : collOpts
    }, 
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}

async function getColl( dbName, collName, options ) { 
  return await persistence.getColl( dbName, collName, options )
}

async function renameColl( jobId, dbName, oldCollName, newCollName ) { 
  await pubsub.sendDbOp( jobId, 
    'rename collection', 
    { 
      dbName      : dbName,
      oldCollName : oldCollName,
      newCollName : newCollName
    }, 
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}

async function getCollSpec( dbName, collName ) { 
  return await persistence.getCollSpec( dbName, collName )
}

async function delColl( jobId, dbName, collName ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 
    'drop collection', 
    { 
      dbName   : dbName,
      collName : collName
    },
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}

// ----------------------------------------------------------------------------

async function nodesUpdCollIdx( jobId, dbName, collName, idx ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 
    'update index', 
    { 
      dbName   : dbName,
      collName : collName,
      idx      : idx
    },
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}


async function reIndexColl( jobId, dbName, collName ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 
    're-index', 
    { 
      dbName   : dbName,
      collName : collName
    },
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}


async function listCollIdx( dbName, collName ) {
  return await persistence.listCollIdx( dbName, collName )
}

async function delCollIdx( dbName, collName, idxName, jobId ) {
  if ( ! jobId ) { jobId = 'OP.'+ helper.randomChar( 10 ) }
  await pubsub.sendDbOp( jobId, 
    'drop index', 
    { 
      dbName   : dbName,
      collName : collName,
      idxlName : idxName
    },
    REQ_REPLY
  )
  let result = await pubsub.getReplies( jobId )
  return result
}
