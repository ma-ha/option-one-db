const log     = require( '../helper/logger' ).log
const helper  = require( './db-helper' )
const pubsub   = require( '../cluster-mgr/pubsub' )

const persistence = require( './db-persistence' )
const { getAllDoc } = require( './db-doc-find' )

module.exports = {
  insertQ,
  insertOneDoc,
  saveErrLog,
  addAuditLog,
  // insertOneDocAllNodes,
  creInsertMsg,
  creDocByIdMsg
}

const PREP_INSERT = 1
const PREP_UPDATE = 2

async function insertQ( insReq ) {
  log.debug( insReq.txnId, 'DB insert', insReq.db, insReq.col )
  let resultData = await persistence.insertDocPrep( insReq.txnId, insReq.db, insReq.col, insReq.doc, insReq.opts )
  return resultData
}


async function saveErrLog( log ) {
  await insertOneDoc( { db: 'admin', coll: 'log' }, log )
}


async function insertOneDoc( r, doc ) {
  log.debug( 'insertOneDoc', r, doc  )
  let dbgIdA = helper.dbgStart( 'insertOneDoc_' )
  let dbgId  = helper.dbgStart( 'insertOneDoc_1' )
  if ( ! r.txnId ) { r.txnId = 'INS.'+helper.randomChar( 10 ) }
  if ( ! r.dt ) { r.dt = Date.now() }
  let collSpec = await persistence.getCollSpec( r.db, r.coll )
  if ( ! collSpec ) { // can happen if cluster is not fully initialized 
    return { _error : 'Collection not initialized' }
  }
  helper.dbgStep( 'insertOneDoc_1', 'insertOneDoc_2', dbgId )
  doc = await prepareDoc( r.db , r.coll, doc, r.txnId )
  helper.dbgStep( 'insertOneDoc_2', 'insertOneDoc_3', dbgId )
  // log.info( 'getCollSpec',collSpec )
  let violatesIdx = await violatesAnyUniqueIdx( r.txnId, r.db, r.coll, collSpec.idx, doc )
  // log.info ( 'insertOneDoc', collSpec, violatesIdx )
  if ( violatesIdx ) {
    // log.info ( 'insertOneDoc', collSpec, violatesIdx )
    helper.dbgEnd( 'insertOneDoc_3', dbgId )
    helper.dbgEnd( 'insertOneDoc', dbgIdA )
    return {
      _error : violatesIdx
    }
  }
  helper.dbgStep( 'insertOneDoc_3', 'insertOneDoc_4', dbgId )
  let { insMsg, token } = await creInsertMsg( r, doc )
  if ( collSpec.masterData ) {
    pubsub.sendRequestAllNodes( r.txnId, insMsg )
  } else {
    pubsub.sendRequest( r.txnId, token, insMsg )
  }
  if ( r.options?.waitForResult === false ) {
    log.debug( r.txnId, 'insertOneDoc', 'waitForResult: false' )
    helper.dbgEnd( 'insertOneDoc_4', dbgId )
    helper.dbgEnd( 'insertOneDoc', dbgIdA )
    return { _ok: true }
  }
  helper.dbgStep( 'insertOneDoc_4', 'insertOneDoc_5', dbgId )
  let result = await pubsub.getReplies( r.txnId )
  helper.dbgEnd( 'insertOneDoc_5',dbgId )
  helper.dbgEnd( 'insertOneDoc', dbgIdA )
  return result
}


async function addAuditLog( sp, cat, obj, event, txnId ) {
  log.debug( txnId, 'audit log...' ) 
  try {
    await insertOneDoc(
      { 
        txnId : 'AUD.'+helper.randomChar( 10 ),
        db    : 'admin', 
        coll  : 'audit-log'
      },
      { 
        ts    : Date.now(),
        sp    : sp,
        cat   : cat,
        obj   : obj,
        event : event,
        txn   : txnId
      } 
    ) 
  } catch (error) { log.error( txnId, 'db.addAuditLog', error ) }
}


async function violatesAnyUniqueIdx( txnId, db, coll, idx, doc ) {
  log.debug( txnId, '>>>>>>>>>>>>>>>>>>>>>>>>', db, coll, idx, doc  )
  if ( ! idx ) { return false }
  for ( let idxKey in idx ) {
    if ( idx[ idxKey ].unique ) {
      if ( doc[ idxKey ] == undefined ||  doc[ idxKey ] == null ) {
        return 'Unique index "'+idxKey+'" violation: Value undefined.'
      } 
      let qryDuplicate = {}
      qryDuplicate[ idxKey ] = doc[ idxKey ]
      let docExists = await getAllDoc( txnId, db, coll, qryDuplicate, [idxKey] )
      log.debug( txnId, 'viol',qryDuplicate , docExists )
      if ( docExists.doc && docExists.doc.length != 0 ) {
        return 'Unique index "'+idxKey+'" violation: Value "'+doc[ idxKey ]+'" already exists.'
      }
    }
  }
  return false
}


// async function insertOneDocAllNodes( r, doc ) {
//   if ( ! r.txnId ) { r.txnId = 'INS.'+helper.randomChar( 10 ) }
//   let { insMsg, token } = await creInsertMsg( r, doc )
//   pubsub.sendRequestAllNodes( r.txnId, insMsg )
//   let result = await pubsub.getReplies( r.txnId )
//   return result
// }

async function creInsertMsg( r, doc ) {
  if ( ! r.txnId ) { r.txnId = 'INS.'+helper.randomChar( 10 ) }
  log.debug( r.txnId, 'DB creInsertMsg',  r.db, r.coll ) 
  let insDoc = await prepareDoc( r.db , r.coll, doc, r.txnId )
  insDoc._cre = r.dt 
  insDoc._chg = r.dt
  let insMsg = {
    op    : 'insert',
    txnId : r.txnId, 
    db    : r.db,
    col   : r.coll,
    doc   : insDoc,
    opt   : r.options
  }
  log.debug( r.txnId, 'DB insert msg', insMsg )
  return { insMsg: insMsg, token: insDoc._token }
}

async function creDocByIdMsg( r, id ) {
  log.info( r.txnId, 'DB creDocByIdMsg', r.db, r.coll, id ) 
  let msg = {
    op    : 'get by id',
    txnId : r.txnId,
    db    : r.db,
    col   : r.coll,
    docId : id,
    upd   : r.update,
    opt   : r.options
  }
  return msg
}


async function prepareDoc( dbName, collName, doc, txnId, creMode ) {
  log.debug( txnId, 'prepareDoc', dbName, collName )
  // ensure _id is set

  if ( ! doc._id ) { 

    let pk = await persistence.getCollPK( dbName, collName )
    log.debug( txnId, 'prepareDoc', pk )
    if ( ! pk || pk[0] === '_id' ) {// if no pk, then generate uuid
      
      doc[ '_id' ] = helper.randomHex( 24 )
      doc[ '_token' ] = helper.extractToken( doc[ '_id' ] )

    } else if ( pk.length == 0 || pk[0] === '' ) {// if no pk, then generate uuid
      
        doc[ '_id' ] = helper.randomHex( 24 )
        doc[ '_token' ] = helper.extractToken( doc[ '_id' ] )
  
    } else {
      
      let pKeyHash = await helper.getPkHash( dbName, collName, doc, pk )
      doc[ '_id' ] = pKeyHash //+'_'+ helper.randomChar( 10 ) 
      // get token from primary key
      doc[ '_token' ] = helper.extractToken( pKeyHash )

    }

  } else  if ( ! doc[ '_token' ] ) {

    doc[ '_token' ] = helper.extractToken( doc[ '_id' ] )

  }
  doc._txnId = txnId
  log.debug( txnId, 'prepareDoc', dbName, collName, doc )
  
  if ( creMode == PREP_INSERT ) { 
    doc._cre = now 
  }
  return doc
}
