const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log
const db      = require( '../db-engine/db' )

const manageDB = require( '../db-engine/db-mgr' )
const backup   = require( '../db-engine/db-backup' )

const { httpSatusCodes : st }  = require( './http-codes' )

module.exports = {
  init,
  createDB,
  listDBs,
  createCollection,
  createIndex,
  listIndexes,
  dropIndex,
  listCollections,
  dropCollection,
  dropDatabase,

  getBackups,
  createBackup,
  restoreBackup,
  getBackupSchedule,
  addBackupSchedule,
  delBackupSchedule
}

// ============================================================================

let cfg = {
  // no config -- yet
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================

async function createDB( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try{
    log.info( jobId, 'DB-API createDB', req.body )

    let mustHave = [ 'body.name' ]
    if ( ! await checkReq( jobId, req, res, 'createDB', mustHave ) ) { return }

    if ( req.body.name === 'admin' ) {
      return res.status( st.BAD_REQUEST ).send( { _error: '"admin" is a reserved DB name"' } )
    }

    if ( ! req.xUserAuthz[ '*' ] ) {
      return res.status( st.NOT_AUTHORIZED ).send( { _error: 'Not authorize.' } )
    }

    if ( await db.getDB( req.body.name ) ) {
      return res.status( st.OK ).send( { _ok: 'DB already exists' } )
    }

    await manageDB.creDB( req.body.name, jobId )
    db.addAuditLog( req.xUser, 'db', req.body.name, 'Create database', jobId )

    res.status( st.ACCEPTED ).send({ _ok: true })
  } catch ( exc ) { sndSendSvrErr( jobId, 'createDB', exc, res ) }
}


async function listDBs( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try{
    log.info( jobId, 'DB-API listDBs' )
    let result = await db.listDBs( )
    let dbList = []
    for ( let dbName of result ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        dbList.push( dbName )
      }
    }
    res.send( dbList )
  } catch ( exc ) { sndSendSvrErr( jobId, 'listDBs', exc, res ) }
}


async function createCollection( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try{
    log.info( jobId, 'DB-API creColl', req.params.db , req.body )
    //let mustHave = [ 'body.collection', 'body.options', 'body.options.primaryKey' ]
    let mustHave = [ 'body.collection' ]
    if ( ! await checkReq( jobId, req, res, 'createCollection', mustHave, req.params.db ) ) { return }

    if ( await manageDB.getColl( req.params.db, req.body.collection ) ) {
      return res.status( st.OK ).send( { _ok: 'Collection already exists' } )
    }
    let newCollOptions = {}
    if ( req.body.options?.primaryKey ) {
      newCollOptions.pk = req.body.options.primaryKey
    } else {
      newCollOptions.noPK = true
      newCollOptions.pk   = ['_id']
    }
    let result = await manageDB.creColl(jobId,  req.params.db, req.body.collection, newCollOptions, jobId ) 
    log.info( jobId, 'creColl', result )
    db.addAuditLog( req.xUser, 'db', req.params.db+'/'+req.body.collection, 'Create collection', jobId )

    res.status( ( result._ok ? st.ACCEPTED : st.BAD_REQUEST ) ).send( result )
  } catch ( exc ) { sndSendSvrErr( jobId, 'createCollection', exc, res ) }
}


async function createIndex( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try{
    log.info( jobId, 'DB-API createIndex', req.params.db, req.params.coll, req.params.field )
    let mustHave = [ 'params.db', 'params.coll', 'params.field' ]
    if ( ! await checkReq( jobId, req, res, 'createIndex', mustHave, req.params.db, req.params.coll ) ) { return }
    let options = {}
    if ( req.body?.options ) { options = req.body.options }

    let collIdx = await manageDB.listCollIdx( req.params.db, req.params.coll, null ) 
    log.info( jobId, 'createIndex', collIdx )
    if ( collIdx._ok && collIdx.index ) {
      // TODO merge idx
      collIdx.index[ req.params.field ] = options
      let result = await manageDB.nodesUpdCollIdx( jobId, req.params.db, req.params.coll, collIdx.index )
      log.info( jobId, 'createIndex', result )
      log.info( jobId, 'createIndex', result )
      res.status( ( result._ok ? st.CREATED : st.SERVER_ERROR ) ).send( result )
    } else {
      res.status( st.SERVER_ERROR ).send({_error : collIdx._error })
    }
  } catch ( exc ) { sndSendSvrErr( jobId, 'createIndex', exc, res ) }
}

async function dropIndex( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try{
    log.info( jobId, 'DB-API dropIndex', req.params.db, req.params.coll, req.params.field )
    let mustHave = [ 'params.db', 'params.coll', 'params.field' ]
    if ( ! await checkReq( jobId, req, res, 'createIndex', mustHave, req.params.db, req.params.coll ) ) { return }

    let collIdx = await manageDB.listCollIdx( req.params.db, req.params.coll, null ) 
    log.info( jobId, 'dropIndex', collIdx )
    if ( collIdx._ok && collIdx.index ) {
      // TODO merge idx
      delete collIdx.index[ req.params.field ] 
      let result = await manageDB.nodesUpdCollIdx( jobId, req.params.db, req.params.coll, collIdx.index )
      log.info( jobId, 'dropIndex', result )
      log.info( jobId, 'dropIndex', result )
      res.status( ( result._ok ? st.CREATED : st.SERVER_ERROR ) ).send( result )
    } else {
      res.status( st.SERVER_ERROR ).send({_error : collIdx._error })
    }
  } catch ( exc ) { sndSendSvrErr( jobId, 'dropIndex', exc, res ) }
}

async function listIndexes( req, res )  {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'DB-API listIndexes', req.params.db, req.params.coll )
    let mustHave = [ 'params.db', 'params.coll' ]
    if ( ! await checkReq( jobId, req, res, 'listIndexes', mustHave, req.params.db, req.params.coll ) ) { return }

    let result = await manageDB.listCollIdx( req.params.db, req.params.coll, null ) 
    log.info( 'listIndex', result )
    if ( result._ok ) {
      let idxArr = {}
      idxArr[ '_PK' ] = result.primaryKey
      for ( let idxKey in result.index ) {
        idxArr[ idxKey ] = result.index[ idxKey ]
      }
      res.status( st.CREATED ).send( idxArr )
    } else {
      let idxArr = []
      res.status( st.SERVER_ERROR ).send( result )
  
    }
  } catch ( exc ) { sndSendSvrErr( jobId, 'listIndexes', exc, res ) }
}


async function listCollections( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'DB-API listCollections', req.params.db )
    let mustHave = [ 'params.db' ]
    if ( ! await checkReq( jobId, req, res, 'listCollections', mustHave, req.params.db ) ) { return }

    let result = await manageDB.getColl( req.params.db, null, null ) 
    res.status( ( result._ok ? st.CREATED : st.SERVER_ERROR ) ).send( result.collections )
  } catch ( exc ) { sndSendSvrErr( jobId, 'listCollections', exc, res ) }
}


async function dropCollection( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'DB-API dropCollection', req.params.db, req.params.coll )
    let mustHave = [ 'params.db', 'params.coll' ]
    if ( ! await checkReq( jobId, req, res, 'dropCollection', mustHave, req.params.db, req.params.coll ) ) { return }

    let result = await manageDB.delColl( jobId, req.params.db, req.params.coll )
    db.addAuditLog( req.xUser, 'db', req.params.db+'/'+req.params.coll, 'Drop collection', jobId )

    res.status( ( result._ok ? st.CREATED : st.SERVER_ERROR ) ).send( result )
  } catch ( exc ) { sndSendSvrErr( jobId, 'dropCollection', exc, res ) }
}


async function dropDatabase( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'DB-API dropDatabase', req.params.db  )
    let mustHave = [ 'params.db' ]
    if ( ! await checkReq( jobId, req, res, 'dropDatabase', mustHave, req.params.db ) ) { return }

    let allSPs = await db.find( 'admin', 'api-access' )
    if ( allSPs._ok ) {
      for ( let sp of allSPs.data ) {
        if ( sp.db ==  req.params.db ) {
          log.info( jobId, 'delete API access id',  sp.db+'/'+sp._id )
          let r = { 
            fn: 'deleteData',
            db : 'admin', 
            coll: 'api-access',
            txnId : jobId,
            options : {}
          }
          await db.deleteOneDoc( r, sp._id )
          db.addAuditLog( req.xUser, 'db', sp.db+'/'+sp._id, 'Del API access', jobId )  
        }
      }
    }


    await manageDB.delDB( req.params.db, jobId )
    db.addAuditLog( req.xUser, 'db', req.params.db, 'Drop database', jobId )

    res.status( st.ACCEPTED ).send({ _ok: true })
  } catch ( exc ) { sndSendSvrErr( jobId, 'dropDatabase', exc, res ) }
}

// ============================================================================

async function getBackups( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    let backups = await backup.getBackups()
    let result = []
    let backupById = {}
    for ( let backup of backups.doc ) {
      let id = ( backup.backupId ? backup.backupId : backup._id )
      if ( ! backupById[ id ] ) { backupById[ id ] = [] }
      backupById[ id ].push( backup )
    }
    for ( let bId in backupById ) {
      let backup = { size: '', status: null }
      let restoreOK = true
      let backupStats = {}
      let sizeMax = 0
      for ( let podBackup of backupById[ bId ] ) {
        backup.id        = podBackup._id
        backup.date      = podBackup.date
        backup.location  = podBackup.location
        backup.dbName    = podBackup.dbName
        backup.retention = podBackup.retention
        backup.date      = podBackup.date
        if ( ! backupStats[ podBackup.status ] ) { backupStats[ podBackup.status ] = 0 }
        backupStats[ podBackup.status ] ++
        let size = parseFloat( podBackup.size )
        if ( size != NaN  && size > sizeMax ) {
          sizeMax = size
        }
        if ( podBackup.status != 'OK ') { restoreOK = false }
      }
      backup.size = '~' + sizeMax + ' MB'
      backup.status = ''
      for ( let stat in backupStats ) {
        if ( backup.status != '') {  backup.status += ' / ' }
        backup.status += backupStats[stat] +' x '+ stat 
      }
      if ( backup.status.indexOf('ERASED') >= 0 ) {
        backup.size = ''
        backup.restore = false
      }
      backup.restore = restoreOK
      result.push( backup )
    }
    result.sort( ( a, b ) => {
      if ( a.date < b.date ) { return 1 } else { return -1 }
    })
    res.send( result )
  } catch ( exc ) { sndSendSvrErr( jobId, 'getBackups', exc, res ) }
}

async function createBackup( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.debug( 'createBackup', req.body )
    if ( ! req.body.dbName ) { return res.send( 'ERROR: DB name required!' )}
    if ( ! req.body.dest ) { return res.send( 'ERROR: Destination required!' )}
    if ( ! db.getDB( req.body.dbName ) ) { return res.send( 'ERROR: DB not fond!' )}
    if ( req.body.collName != '*' ) {
      if ( ! await db.getColl( req.body.dbName, req.body.collName ) ) {
        return res.send( 'ERROR: Collection not fond!' )
      }
    }
    let result = await backup.createBackup(
      req.body.dest,
      req.body.dbName,
      req.body.collName,
      req.body.retention,
    )
    res.send('Started ...')
  } catch ( exc ) { sndSendSvrErr( jobId, 'createBackup', exc, res ) }
}

async function restoreBackup( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'restoreBackup' )
    if ( ! req.body.source ) { return res.send( 'ERROR: Source required!' )}
    if ( ! req.body.backupDate ) { return res.send( 'ERROR: Date required!' )}
    if ( ! req.body.dbName ) { return res.send( 'ERROR: DB name required!' )}
    if ( ! req.body.collName ) { return res.send( 'ERROR: DB collection required!' )}
    await backup.startRestoreJob(
      req.body.backupDate, 
      req.body.source, 
      req.body.dbName, 
      req.body.collName, 
      req.body.restoreIndex, 
      req.body.deactivateExpire
    )
    res.send( 'Restore job started.' )
  } catch ( exc ) { sndSendSvrErr( jobId, 'restoreBackup', exc, res ) }
}

async function getBackupSchedule( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    let backupScheduleArr = await backup.getBackupSchedule()
    res.send( backupScheduleArr.doc )
  } catch ( exc ) { sndSendSvrErr( jobId, 'getBackupSchedule', exc, res ) }
}

async function addBackupSchedule( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    log.info( jobId, 'addBackupSchedule', req.body )
    if ( ! req.body.dbName ) { return res.send( 'ERROR: DB name required!' )}
    if ( ! req.body.dest ) { return res.send( 'ERROR: Destination required!' )}
    if ( ! db.getDB( req.body.dbName ) ) { return res.send( 'ERROR: DB not fond!' )}
    if ( req.body.collName != '*' ) {
      if ( await db.getColl( req.body.dbName, req.body.collName ) ) { return res.send( 'ERROR: Collection not fond!' )}
    }    if ( ! req.body.schedule ) { return res.send( 'ERROR: Schedule required!' )}
    // TODO validate schedule
    let result = await backup.addBackupSchedule(
      req.body.schedule,
      req.body.dest,
      req.body.dbName,
      req.body.collName,
      req.body.retention,
    )
    if ( result._error ) { return  res.send('ERROR: '+result._error ) }
    res.send( 'Schedule added.' )
  } catch ( exc ) { sndSendSvrErr( jobId, 'addBackupSchedule', exc, res ) }
}

async function delBackupSchedule( req, res ) {
  let jobId = db.getTxnId( 'OP' )
  try {
    if ( ! req.query._id ) { return res.send( 'ERROR: ID required!' )}
    let result = await backup.delBackupSchedule( req.query._id )
    if ( result._error ) { return res.send('ERROR: '+result._error ) }
    res.send( 'Schedule deleted.' )
  } catch ( exc ) { sndSendSvrErr( jobId, 'delBackupSchedule', exc, res ) }
}

// ============================================================================
// helper

function paramsOK( req, res, r ) {
  if ( ! req.params.db ) {
    res.send( { error: 'db required' } )
    return false
  } else if ( ! req.params.coll ) {
    res.send( { error: 'collection required' } )
    return false
  } 
  r.db   = req.params.db
  r.coll = req.params.coll 
  return true
}


function docOK( req, res, r ) {
  if ( ! req.body || ! req.body.doc ) {
    res.send( { error: 'doc required' } )
    return false
  } else if ( ! req.body.options ) {
    res.send( { error: 'options required' } )
    return false
  } 
  r.doc     = req.body.doc
  r.options = req.body.options
  return true
}


function queryOK( req, res, r ) {
  if ( ! req.body || ! req.body.doc ) {
    res.send( { error: 'doc required' } )
    return false
  } else if ( ! req.body.options ) {
    res.send( { error: 'options required' } )
    return false
  } 
  r.doc     = req.body.doc
  r.options = req.body.options
  return true
}


async function checkReq( jobId, req, res, fnName, mustHave, dbName, collName ) {
  // check authorization for dbName
  if ( ! req.xUserAuthz[ '*' ] ) { 
    if ( ! req.xUserAuthz[ dbName ] ) { 
      log.warn( jobId, fnName, req.xUser ,'not authorzed for', dbName )
      return res.status( st.NOT_AUTHORIZED ).send( { error: 'Not authorized' } ) 
    }
  } // ok, authz for all DBs 

  // validate parameters
  for ( let param of mustHave ) {
    if ( ! resolve( req, param ) ) {
      log.warn( jobId, fnName, '"'+param+'" required' )
      res.status( st.BAD_REQUEST ).send( { error: '"'+ param +'" required' } ) 
      return false
    }
  }
  if ( dbName ) {
    if ( ! validName( dbName ) ){ 
      return sndBadRequest( jobId, res, fnName, 'DB "'+ dbName +'" name not valid' ) 
    }
    if ( ! await db.getDB( dbName ) ) { 
      return sndNotFound( jobId, res, fnName, 'DB "'+ dbName +'" not found' ) 
    }
  }
  if ( collName ) {
    if ( ! validName( collName ) ){ 
      return sndBadRequest( jobId, res, fnName, 'DB "'+ dbName +'" name not valid' ) 
    }
    if ( ! await manageDB.getColl( dbName, collName ) ) { 
      return sndNotFound( jobId, res, fnName, 'Collection "'+ dbName+'.'+collName +'" not found' )  
    }
  }
  return true
}


function resolve( obj, path ) {
  var rv = path.split( '.' ).reduce( (o, p) => {
    return o && o[p]
  }, obj ) 
  return rv || false
}


function sndBadRequest( jobId, res, fnName, errTxt ) {
  log.warn(  jobId, fnName, 'Bad request', errTxt )
  res.status( st.BAD_REQUEST ).send( { error: errTxt } ) 
}


function sndNotFound( jobId, res, fnName, errTxt ) {
  log.warn(  jobId, fnName, 'Bad request', errTxt )
  res.status( st.NOT_FOUND ).send( { error: errTxt } ) 
}

function sndSendSvrErr( jobId, method, exc, res ) {
  log.warn(  jobId, 'API', method, exc )
  res.status( st.SERVER_ERROR ).send()
}

function validName( name ) {
  if ( ! /^[a-zA-Z0-9-_]+$/.test( name ) ) {
    return false
  } else {
    return true
  }
}
