const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log
const db      = require( '../db-engine/db' )
const pubsub  = require( '../cluster-mgr/pubsub' )
const helper  = require( '../db-engine/db-helper' )
const apiHelper = require( './api-helper' )

const dbDocUpd  = require( '../db-engine/db-doc-upd' )
const dbDocFind = require( '../db-engine/db-doc-find' )

// const persistence = require( '../db-engine//db-persistence' )

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
  deleteById,

  addAttachment,
  getAttachments,
  getAttachment,
  deleteAttachment
}


// ============================================================================

let cfg = {
  // no config -- yet
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================

async function insert( req, res ) { // OK
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

async function find( req, res ) { // OK
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


async function countDocuments( req, res ) { // OK
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


async function getDocById( req, res ) { // OK
  try {
    let r = { fn: 'getDocById' }
    if ( await paramsOK( req, res, r, 'GET' )  && req.params.id  ) {
      log.info( r.txnId, 'API find by id', req.params.db, req.params.coll, req.params.id )

      let response = await dbDocFind.findOneDoc( r, { _id: req.params.id } )

      // let token = helper.extractToken( req.params.id )
      // let qryMsg = await db.docByIdMsg( r, req.params.id )
      // pubsub.sendRequest( r.txnId, token, qryMsg )

      // let result = await pubsub.getReplies( r.txnId )
      // let response = db.creDocsFoundResponse( r.txnId, result )
      log.info( r.txnId, 'API find by id response', req.params.id, 'cnt=' + response.dataLength )
      res.send( response )
    }
  } catch ( exc ) { sndSendSvrErr( 'getDocById', exc, res ) }
}

// ----------------------------------------------------------------------------
// {
//   "options": { "one": true }
//   "filter": {
//     "_id": "0dd8f833cedfd48582693331"
//   },
//   "update": {
//     "$set": {
//       "name": "John Doe"
//     }
//   }
// }

async function update( req, res ) { // OK
  try {
    let r = { fn: 'updateOne' }
    if ( await paramsOK( req, res, r, 'UPD' ) && updateOK( req, res, r ) ) {
      log.info( r.txnId, 'API.update', req.params, req.body ) 
    

      if ( req.body.options?.one ) { // update one
        let origDoc = null
        if (  r.filter._id ) {
          // let find = await db.findOneDoc( r, r.filter )
          let find = await dbDocFind.findOneDoc( r, r.filter )
          log.debug( 'API.update', find )
          if ( find._error ) { return res.status( st.NOT_FOUND ).send() }
          origDoc = find.doc
        } else {
          // TODO filter for one
          return res.status( st.BAD_REQUEST ).send()
        }

        let result = await dbDocUpd.updateOneDoc( r, { _id: origDoc._id }, origDoc )
        // dbMetrics.addDbMetric( r.db, r.coll, "upd", result )
  
        // // TODO check !!!
        // let result = await db.updateOneDoc( r, doc )

        log.debug( 'API.updateOne', result )
        if ( result._ok ) {
          res.send( result )
        } else {
          res.status( st.SERVER_ERROR ).send( result )
        }
      
      } else { // update many

        log.info( r.txnId, 'API.updateMany',  r.filter ) 
        // let find = await db.find( r.db, r.coll, r.filter, req.body.options )
        let find = await dbDocFind.find( r.db, r.coll, r.filter, req.body.options )
        if ( ! find._error ) {
          let result = {
            _ok        : true, 
            _okCnt  : 0,
            _nokCnt : 0,
            updatedIds : []
          }
          for ( let doc of find.data ) {
            // log.info( 'API update >> ', doc._id ) 

            // let updOne = await db.updateOneDoc( r, doc )
            let updOne = await dbDocUpd.updateOneDoc( r, { _id: doc._id }, doc )
            // dbMetrics.addDbMetric( r.db, r.coll, "upd", updOne )
    
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

async function replaceOne( req, res ) { // TODO
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

async function deleteData( req, res ) { // TODO
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


async function deleteById( req, res ) { // TODO
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

//-----------------------------------------------------------------------------

async function addAttachment( req, res ) { // TODO
  try {
    let r = { fn: 'addAttachment' }
    if ( ! req.files ) { return res.status( st.BAD_REQUEST ).send( 'File required' ) }
    if ( await paramsOK( req, res, r, 'POST' )  && req.params.id  ) {
      log.info( r.txnId, 'API add attachment', r.db, r.coll, req.params.id )
      let attResult = await db.addAttachment(
        r,
        req.params.id,
        req.files.file.name,
        req.files.file.mimetype,
        req.files.file.size,
        req.files.file.data.toString('hex'),
        req.query.label
      )

      if ( attResult._ok ) {
        res.status( st.ACCEPTED ).send( 'OK' )
      } else {
        res.status( st.SERVER_ERROR ).send( 'Failed' )
      }
      
    }
  } catch ( exc ) { sndSendSvrErr( 'addAttachment', exc, res ) }
}


async function getAttachments( req, res ) { // TODO
  try {
    let r = { fn: 'getAttachments' }
    if ( await paramsOK( req, res, r, 'POST' )  && req.params.id  ) {
      log.info( r.txnId, 'API get attachments', req.params.db, req.params.coll, req.params.id )
      let token = helper.extractToken( req.params.id )

      let qryMsg = await db.docByIdMsg( r, req.params.id )
      pubsub.sendRequest( r.txnId, token, qryMsg )

      let result = await pubsub.getReplies( r.txnId )
      let response = db.creDocsFoundResponse( r.txnId, result )
      if ( response._ok && response.dataLength == 1 ) {
        let doc = response.data[ 0 ]
        if ( ! doc._attachment ) { 
          res.send({})
        } else {
          res.send( doc._attachment )
        }
      }
      return sndBadRequest( res, r.fn, 'Document not found' )
    }
  } catch ( exc ) { sndSendSvrErr( 'getAttachments', exc, res ) }
}


async function getAttachment( req, res ) { // TODO
  try {
    let r = { fn: 'getAttachment' }
    if ( await paramsOK( req, res, r, 'POST' )  && req.params.id  && req.params.file ) {
      log.info( r.txnId, 'API get attachment', req.params.db, req.params.coll, req.params.id, req.params.file )
      let token = helper.extractToken( req.params.id )

      let qryMsg = await db.docByIdMsg( r, req.params.id )
      pubsub.sendRequest( r.txnId, token, qryMsg )

      let result = await pubsub.getReplies( r.txnId )
      let response = db.creDocsFoundResponse( r.txnId, result )
      if ( response._ok && response.dataLength == 1 ) {
        let doc = response.data[ 0 ]
        if ( doc._attachment && doc._attachment[ req.params.file ] ) { 
          let attMeta =  doc._attachment[ req.params.file ]
          let attResult = await db.getAttachment(
            {
              fn    : 'getAttachment',
              db    : req.params.db,
              coll  : req.params.coll,
              txnId : apiHelper.randomChar( 10 )
            },
            req.params.id,
            req.params.file
          )
          // log.info( '*****', attResult )
          // log.info( '*****', attResult.replyMsg[0].data )
          // let fileContents = attResult.replyMsg[0].data
          let fileContents = Buffer.from( attResult.replyMsg[0].data, 'hex' )
          
          res.set('Content-disposition', 'attachment; filename=' + req.params.file )
          if ( attMeta._mimetype ) {
            res.set('Content-Type', attMeta._mimetype )
          }
          // if ( attMeta._size ) {
          //   res.set('Content-Size', attMeta._size )
          // }
          var stream = require( 'stream' )
          var readStream = new stream.PassThrough()
          readStream.end( fileContents )
          readStream.pipe( res )
          return

        } else {
          return sndBadRequest( res, r.fn, 'Attachment not found' )
        }
      }
      return sndBadRequest( res, r.fn, 'Document not found' )
    }
  } catch ( exc ) { sndSendSvrErr( 'getAttachment', exc, res ) }
}

async function deleteAttachment( req, res ) { // TODO
  try {
    let r = { fn: 'getAttachment' }

    let getDoc = await db.getDocById( req.params.db, req.params.coll, req.params.id )
    if ( ! getDoc._ok ) {
      return res.status( st.NOT_FOUND ).send( 'doc not found' )
    }
    if ( ! getDoc.doc._attachment || ! getDoc.doc._attachment[ req.params.file ] ) {
      return res.status( st.NOT_FOUND ).send( 'attachment not found' )
    }

    let delResult = await db.deleteAttachment(
      {
        fn    : 'delAttachment',
        db    : req.params.db,
        coll  : req.params.coll,
        txnId : apiHelper.randomChar( 10 )
      },
      req.params.id,
      req.params.file
    )

    if ( delResult._ok ) {
      return res.status( st.OK ).send()
    } else {
      return res.status( st.SERVER_ERROR ).send(  'ERROR: '+ delResult._error )
    }

  } catch ( exc ) { sndSendSvrErr( 'getAttachment', exc, res ) }
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
  let proj = null
  if ( req.query?.projection ) { 
    proj = req.query.projection
  } else if ( req.body?.projection ) { 
    proj = req.body.projection
  }
  let options = {}
  if ( req.query?.options ) { 
    options = req.query.options
  } else if ( req.body?.options ) { 
    options = req.body.options
  }
  r.db      = req.params.db
  r.coll    = req.params.coll 
  r.dt      = Date.now()
  r.txnId   = ( txnPrefix ? txnPrefix + '.' : '' ) + helper.randomChar( 10 )
  r.proj    = proj
  r.options = options
  return true
}


function docOK( req, res, r ) {
  if ( ! req.body || ! req.body.doc ) {
    return sndBadRequest( res, r.fn, 'doc required' )
  } 
  // else
  //   return sndBadRequest( res, r.fn, 'options required' )
  // } 

  if ( Array.isArray( req.body.doc ) ) {
    r.doc  = req.body.doc
  } else if ( typeof req.body.doc === 'object' ) {
    r.doc  = [ req.body.doc ]
  }
  
  if ( req.body.options ) {
    r.options = req.body.options
  } else {
    r.options = {}
  }
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
  // if ( ! req.body.options ) { return sndBadRequest( res, r.fn, 'options required' ) }
  // TODO: check update syntax
  r.filter   = req.body.filter
  r.update   = req.body.update
  r.options  =  ( req.body.options ? req.body.options : {} )
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

