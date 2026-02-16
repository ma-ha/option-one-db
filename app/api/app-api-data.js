const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log
const db      = require( '../db-engine/db' )
const pubsub  = require( '../cluster-mgr/pubsub' )
const helper  = require( '../db-engine/db-helper' )

const persistence = require( '../db-engine//db-persistence' )

const { httpSatusCodes : st }  = require( './http-codes' )

module.exports = {
  init,
  insert,
  find,
  getDocById,
  // update,
  update,
  replaceOne,
  // updateMany,
  countDocuments,
  deleteData,
  deleteById
}


// ============================================================================

let cfg = {
  // no config -- yet
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================

async function insert( req, res ) {
  let tId = helper.dbgStart( 'insert' )
  try {
    let r = { fn: 'insert' }
    if ( await paramsOK( req, res, r, 'INS' ) && docOK( req, res, r ) ) {
      log.info( r.txnId, 'API insert', req.params ) 
      let allInserts = []
      for ( let doc of r.doc ) {
        allInserts.push( db.insertOneDoc( r, doc ) )
      }
      let tId2 = helper.dbgStart( 'insert_p_allSettled' )
      let insertResult = await Promise.allSettled( allInserts )
      helper.dbgEnd( 'insert_p_allSettled', tId2 )
      res.status( st.ACCEPTED ).send({ _ok : true, result: insertResult }) // TODO add doc status to result
      helper.dbgEnd( 'insert', tId )
      if ( r.options.printDebugTimes ) { helper.dbgPrint() }
    } // else res already sent
  } catch ( exc ) { sndSendSvrErr( 'insert', exc, res ) }
}

// ============================================================================

async function find( req, res ) {
  try {
    let r = { fn: 'find' }
    if ( await paramsOK( req, res, r, 'FND' ) ) {
      log.info(  r.txnId, 'API find...' ) //, req.query )
      let response = await db.findDocs( r, req.query.query ) 
      // log.info( r.txnId, 'API find response', response )
      log.info( r.txnId, 'API find response', 'cnt=', response.docIds?.length )
      res.send( response )
    } // else res already sent
  } catch ( exc ) { sndSendSvrErr( 'find', exc, res ) }
}


async function countDocuments( req, res ) {
  try {
    let r = { fn: 'find' }
    if ( await paramsOK( req, res, r, 'CNT`' ) ) {
      log.info( r.txnId, 'API count...', req.query )
      r.options = { idsOnly : true }
      let response = await db.findDocs( r, req.query.query ) 
      log.info( r.txnId, 'API find response', 'cnt=', response.docIds.length )
      if ( response._ok ) {
        res.send({ 
          _ok   : true,
          count : response.docIds.length
        })
      } else {
        res.send({ _error: response._error })
      }
    } // else res already sent
  } catch ( exc ) { sndSendSvrErr( 'find', exc, res ) }
}


async function getDocById( req, res ) {
  try {
    let r = { fn: 'getDocById' }
    if ( await paramsOK( req, res, r, 'GET' )  && req.params.id  ) {
      log.info( r.txnId, 'API find by id', req.params.db, req.params.coll, req.params.id )
      let token = helper.extractToken( req.params.id )
      let qryMsg = await db.creDocByIdMsg( r, req.params.id )
      pubsub.sendRequest( r.txnId, token, qryMsg )

      let result = await pubsub.getReplies( r.txnId )
      let response = db.creDocsFoundResponse( r.txnId, result )
      log.info( r.txnId, 'API find by id response', req.params.id, 'cnt=' + response.dataLength )
      res.send( response )
    }
  } catch ( exc ) { sndSendSvrErr( 'getDocById', exc, res ) }
}


async function update( req, res ) {
  try {
    let r = { fn: 'updateOne' }
    if ( await paramsOK( req, res, r, 'UPD' ) && updateOK( req, res, r ) ) {
      log.info( r.txnId, 'API update', req.params ) 

      if ( req.body.options?.one ) { // update one
        let doc = { _id : r.filter._id }
        if ( ! doc._id ) {
          let find = await db.findOneDoc( r, r.filter )
          if ( find._error ) { return res.status( st.BAD_REQUEST ).send( find ) }
          doc = find.doc
        }
        let result = await db.updateOneDoc( r, doc )
        //log.info( 'API updateOne', result )
        if ( result._ok ) {
          res.send( result )
        } else {
          res.status( st.BAD_REQUEST ).send( result )
        }
      
      } else { // update many

        log.info( r.txnId, 'API update many...',  r.filter ) 
        let find = await db.find( r.db, r.coll, r.filter, req.body.options )
        if ( ! find._error ) {
          let result = {
            _ok        : true, 
            _okCnt  : 0,
            _nokCnt : 0,
            updatedIds : []
          }
          for ( let doc of find.data ) {
            // log.info( 'API update >> ', doc._id ) 

            let updOne = await db.updateOneDoc( r, doc )
            // log.info( 'API update >>>> ',updOne ) 
            if ( updOne._ok ) {
              result._okCnt ++
              result.updatedIds.push( updOne._id )
            } else {
              result._nokCnt ++
            }
          }
          log.info( r.txnId, 'API update many _okCnt', result._okCnt ) 

          res.send( result )
        } else { 
          res.status( st.BAD_REQUEST ).send( find )
        }
      }
    }
  } catch ( exc ) { sndSendSvrErr( 'update', exc, res ) }

}

async function replaceOne( req, res ) {
  try {
    let r = { fn: 'replaceOne' }
    if ( await paramsOK( req, res, r, 'RPL' ) ) {
      log.info( r.txnId, 'API replaceOne', req.params.db, req.params.coll, req.params.id ) 
      let doc = req.body
      let result = await db.replaceOneDoc( r.txnId, r.db, r.coll, req.params.id, doc )
      res.send( result )
    }
  } catch ( exc ) { sndSendSvrErr( 'replaceOne', exc, res ) }
}

async function deleteData( req, res ) {
  log.info( 'API deleteData...', req.query ) 
  try {
    let r = { fn: 'deleteData' }
    if ( await paramsOK( req, res, r, 'DEL' ) ) {
      if ( ! req.query.filter ) {
        return res.status( st.BAD_REQUEST ).send( 'Filter required!' )
      }
      log.info( r.txnId, 'API deleteData...' ) 
      r.filter = req.query.filter 
      let docIDs = []
      if ( r.filter._id ) {
        docIDs.push( r.filter._id )
      } else {
        let find = await db.findDocs( r, req.query.query ) 
        if ( find._error ) { return res.status( st.BAD_REQUEST ).send( find ) }
        docIds = find.docIds
      }
      if ( docIDs.length == 0 ) {
        return res.status( st.BAD_REQUEST ).send( 'No documents found!' )
      } else if ( r.options?.one && docIDs.length != 1 ) {
        return res.status( st.BAD_REQUEST ).send( 'DeleteOne: Found '+docIDs.length+' documents!' ) 
      }

      if ( req.body.options?.one && docIDs.length != 1 ) {
        return res.status( st.BAD_REQUEST ).send( 'DeleteOne: Found '+docIDs.length+' documents!' ) 
      }

      let delDocPromises = []
      for ( let docID of docIDs ) {
        let delDoc = await db.deleteOneDoc( r, docID )
        delDocPromises.push( delDoc )
      }
      let allResults = await Promise.allSettled( delDocPromises )

      res.send( { _ok: true, results: allResults} )
    }
  } catch ( exc ) { sndSendSvrErr( 'deleteOne', exc, res ) }
}


async function deleteById( req, res ) {
  try {
    let r = { fn: 'deleteData' }
    if ( await paramsOK( req, res, r, 'DEL' )  ) {
      log.info( r.txnId, 'API deleteData...' ) 
      res.send( { _ok: true, results: allResults} )
      let delDoc = await db.deleteOneDoc( r, req.params.id )
      res.send( { _ok: true, results: delDoc} )
    }
  } catch ( exc ) { sndSendSvrErr( 'deleteOne', exc, res ) }
}


// ============================================================================
// ============================================================================
// TODOs


// async function update( req, res ) { // TODO: impment in sdk
//   log.info( 'update...' ) 
//   let r = { fn: 'update' }
//   if ( await paramsOK( req, res, r )  && queryOK( req, res, r )  ) {
//     let updateResult = await db.update( r.db, r.coll, r.query, r.options )
//     res.send( updateResult )
//   }
// }


// async function updateMany( req, res ) { // TODO: impment in sdk
//   log.info( 'updateMany...' ) 
//   let r = { fn: 'updateMany' }
//   if ( await paramsOK( req, res, r )  && queryOK( req, res, r ) && req.body.update  ) {
//     let updateResult = await db.updateMany( r.db, r.coll, r.query, req.body.update, r.options )
//     res.send( updateResult )
//   }
// }

// ============================================================================
// helper

async function paramsOK( req, res, r, txnPrefix) {
  if ( ! req.params.db ) {
    return sndBadRequest( res, r.fn, 'DB name required' ) 
  } else if ( ! req.params.coll ) {
    return sndBadRequest( res, r.fn, 'Collection name required' ) 
  } 
  if ( ! await db.getDB( req.params.db ) ) { 
    return sndBadRequest( res, r.fn, 'DB "'+ req.params.db +'" not found' ) 
  } 
  if ( ! await db.getColl( req.params.db, req.params.coll ) ) { 
    return sndBadRequest( res, r.fn, 'Collection "'+ req.params.coll +'" not found' )  
  }
  log.debug( 'paramsOK', req.query )
  r.db      = req.params.db
  r.coll    = req.params.coll 
  r.dt      = Date.now()
  r.txnId   = ( txnPrefix ? txnPrefix + '.' : '' ) + helper.randomChar( 10 )
  r.proj    = ( req.query.projection ? req.query.projection : req.body.projection )
  r.options = ( req.query.options ? req.query.options : req.body.options )
  return true
}


function docOK( req, res, r ) {
  if ( ! req.body || ! req.body.doc ) {
    return sndBadRequest( res, r.fn, 'doc required' )
  } else if ( ! req.body.options ) {
    return sndBadRequest( res, r.fn, 'options required' )
  } 

  if ( Array.isArray( req.body.doc ) ) {
    r.doc  = req.body.doc
  } else if ( typeof req.body.doc === 'object' ) {
    r.doc  = [ req.body.doc ]
  }
  
  r.options = req.body.options
  return true
}


function queryOK( req, res, r ) {
  if ( ! req.body || ! req.body.query ) {
    return sndBadRequest( res, r.fn, 'query required' )
  } else if ( ! req.body.options ) {
    return sndBadRequest( res, r.fn, 'options required' )
  } 
  r.query   = req.body.query
  r.options = req.body.options
  return true
}


function updateOK( req, res, r ) {
  if ( ! req.body ) { return sndBadRequest( res, r.fn, 'body required' )  }
  if ( ! req.body.filter  ) { return sndBadRequest( res, r.fn, 'filter required' ) }
  if ( ! req.body.update  ) { return sndBadRequest( res, r.fn, 'update required' ) }
  if ( ! req.body.options ) { return sndBadRequest( res, r.fn, 'options required' ) }
  // TODO: check update syntax
  r.filter   = req.body.filter
  r.update   = req.body.update
  r.options  = req.body.options
  return true
}

function sndBadRequest( res, fnName, errTxt ) {
  log.warn( 'API', fnName, 'Bad request', errTxt )
  res.status( st.BAD_REQUEST ).send( { error: errTxt } ) 
  return false
}


function sndSendSvrErr( method, exc, res ) {
  log.warn( 'API', method, exc )
  res.status( st.SERVER_ERROR ).send()
}
//-----------------------------------------------------------------------------

function stripTxnId( result ) {
  if ( result && result.doc ) {
    if ( Array.isArray( result.doc ) ) {
      result.doc.forEach( doc => { delete doc._txnId })
    } else {
      delete result.doc._txnId
    }
  }
}

