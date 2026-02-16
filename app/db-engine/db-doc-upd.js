const log     = require( '../helper/logger' ).log
const helper  = require( './db-helper' )
const pubsub   = require( '../cluster-mgr/pubsub' )

const persistence = require( './db-persistence' )

module.exports = {
  updateOneDoc,
  replaceOneDoc,
}

async function updateOneDoc( r, doc, origDoc, opt = {} ) {
  if ( ! r.txnId ) { r.txnId = 'UDP.'+helper.randomChar( 10 ) }
  try {
    let update = r.update
    // if ( r.txnId.startsWith('DMX') ) log.info ( update, origDoc )
    for ( let updOp of  [ '$set', '$unset', '$inc','$min', '$max', '$push', '$addToSet', '$pop','$rename' ] ) {
      if ( update[ updOp ] ) { 
        if ( typeof  update[ updOp ] !== 'object' ) { 
          return { _error: 'update '+updOp+' must be an object' } 
        }
        for ( const key in update[ updOp ] ) {
          let modResult = modField( r.txnId, origDoc, key, update[ updOp ][ key ], updOp )
          if ( modResult._error ) { 
            return { _error: modResult._error } 
          }
        }
      }  
    }
    origDoc._chg   = Date.now()
    origDoc._txnId = r.txnId

    let token  = helper.extractToken( doc._id )
    let updMsg = {
      op    : 'replace one',
      txnId : r.txnId,
      db    : r.db,
      col   : r.coll,
      docId : doc._id,
      doc   : origDoc
    }
    if ( opt?.allNodes ) {
      pubsub.sendRequestAllNodes( r.txnId, updMsg )
    } else {
      pubsub.sendRequest( r.txnId, token, updMsg )
    }
    let result = await pubsub.getReplies( r.txnId )
    return { _ok: result._ok, _error: result._error, _id: doc._id }

  } catch ( error ) {
    log.fatal( r.txnId, 'DB updateOne', error )
    return { _error: error.message }
  }
}

// ============================================================================

async function replaceOneDoc( txnId, dbName, collName, docId, doc, opt ) {
  log.debug( txnId, 'replaceOneDoc', dbName, collName, docId )
  let changedIdxField = []
  if ( ! doc._token ) {
    doc._token = helper.extractToken( doc._id )
  }
  // check if indexed fields are modifies
  let collSPec = await persistence.getCollSpec( dbName, collName )
  let origDoc = await persistence.getDocById( dbName, collName, docId )
  for ( let idxField in collSPec.idx ) {
    if ( origDoc.doc[ idxField ] !== doc[ idxField ] ) {
      changedIdxField.push( idxField )
    }
  }
  // replace doc in db
  await persistence.updateDocPrep( txnId, dbName, collName, doc )
  let result = await persistence.updateDocCommit( txnId, dbName, collName, docId )
  // need index update?
  if ( changedIdxField.length > 0 ) {
    log.info( txnId, 'replaceOneDoc changedIdxField', changedIdxField )
    await persistence.updateDocIndex( txnId, dbName, collName, changedIdxField, doc ) 
  }
  return result
}

// ============================================================================

function modField( txnId, doc, field, value, updateOp ) {
  // if ( txnId.startsWith('DMX') )  log.info( '>>>>>>>>>>>>>  modField', doc, field, value, updateOp )
  let split = field.indexOf('.')
  if ( split > 0 ) {
    let subField = field.substring( 0, split )
    let nextField = field.substring( split + 1 )
    // if ( txnId.startsWith('DMX') )  log.info( '>>>>>>>>>>>>>  modField', split, subField, doc.hasOwnProperty( subField ), nextField )
    if ( ! doc.hasOwnProperty( subField ) ) {
      switch ( updateOp ) {
        case '$set' :  
          doc[ subField ] = {}
          break
        case '$unset' :  
          doc[ subField ] = {}
          break
        case '$inc' :  
          doc[ subField ] = {}
          break
        case '$push' :  
          doc[ subField ] = {}
          break
        default: 
          return { _error: 'Update: sub-property "'+ value +'" not found' }
      }
    }
    let result = modField( txnId, doc[ subField ], nextField, value, updateOp )
    return result
  } else {
    switch ( updateOp ) {
      case '$set' :  
        doc[ field ] = value
        break
      case '$unset' :
        delete doc[ field ]
        break
      case '$inc' : 
        if ( doc[ field ] == undefined ) { doc[ field ] = 0 }
        // log.info ( '$inc >>>> ', doc, field,  isNum( doc[ field ] ), value )
        if ( isNum( doc[ field ] ) ) {
          doc[ field ] += value
        } else {
          doc[ field ] = value
        }
        break
      case '$min' : 
        if ( isNum( doc[ field ] ) &&  doc[ field ] > value ) {
          doc[ field ] = value
        }
        break
      case '$max' :  
        if ( isNum( doc[ field ] ) &&  doc[ field ] < value ) {
          doc[ field ] = value
        }
        break
      case '$push' :  
      if ( ! doc[ field ] ) { doc[ field ] = [] }
        if ( Array.isArray( doc[ field ] ) ) {
          if ( value && value[ '$each' ] && Array.isArray( value[ '$each' ] ) ) {
            for ( let e of value[ '$each' ] ) {
              doc[ field ].push( e )
            }
          } else {
            doc[ field ].push( value )
          }
        }
        break
      case '$addToSet' : 
        if ( Array.isArray( doc[ field ] ) ) {
          if ( value && value[ '$each' ] && Array.isArray( value[ '$each' ] ) ) {
            for ( let e of value[ '$each' ] ) {
              if ( doc[ field ].indexOf( e ) == -1 ) {
                doc[ field ].push( e )
              }
            }
          } else {
            if ( doc[ field ].indexOf( value ) == -1 ) {
              doc[ field ].push( value )
            }
          }
        }
        break
      case '$pop' :  
        if ( Array.isArray( doc[ field ] ) ) {
          if ( value === 1 ) {
            doc[ field ].pop()
          } else if ( value === -1 ) {
            doc[ field ].shift()
          }
        }
        break
      case '$rename' : 
        if ( ! doc.hasOwnProperty( value ) ) {
          doc[ value] = doc[ field ]
          delete  doc[ field ]
        } else {
          return { _error: 'Update $rename: property "'+ value +'" already exists' }
        }
        break
      default: break
    }
  }
  return { _ok: true }
}

function isNum( num ) {
  return ! isNaN( num )
}
