const log     = require( '../helper/logger' ).log
// const hash    = require( 'hash-sum' )
const helper  = require( './db-helper' )
const pubsub   = require( '../cluster-mgr/pubsub' )

const manageDBs = require( './db-mgr' )
const persistence = require( './db-persistence' )


module.exports = {
  deleteDocById,
  deleteOneDoc,
  deleteOneDocAllNodes,
  creDeltMsg
}


async function deleteDocById( jobId, dbName, coll, docId, options ) {
  let result = await persistence.deleteDoc( jobId, dbName, coll, docId, options )
  return result
}

async function deleteOneDoc( r, id ) {
  // log.info( 'deleteOneDoc', r, id)
  let { txnId, deleteMsg, token } = await creDeltMsg( r, id )
  // log.info( 'deleteOneDoc', token, deleteMsg )
  pubsub.sendRequest( txnId, token, deleteMsg )
  let result = await pubsub.getReplies( txnId )
  return result
}


async function deleteOneDocAllNodes( r, id ) {
  let { txnId, deleteMsg, token } = await creDeltMsg( r, id )
  pubsub.sendRequestAllNodes( txnId, deleteMsg )
  let result = await pubsub.getReplies( txnId )
  return result
}

async function creDeltMsg( r, id ) {
  if ( ! r.txnId ) { r.txnId = 'DEL.'+ helper.randomChar( 10 ) }
  log.info(  r.txnId, 'DB creDeltMsg',r.db, r.coll ) 
  let msg = {
    op    : 'delete doc',
    txnId : r.txnId, 
    db    : r.db,
    col   : r.coll,
    docId : id,
    opt   : r.options
  }
  log.debug( 'DB delete msg', msg )
  let token = helper.extractToken( id )
  return { txnId: r.txnId, deleteMsg: msg, token: token }
}

