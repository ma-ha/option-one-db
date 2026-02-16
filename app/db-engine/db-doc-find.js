const log     = require( '../helper/logger' ).log
const hash    = require( 'hash-sum' )
const api     = require( '../helper/api-client' )
const helper  = require( './db-helper' )
const pubsub  = require( '../cluster-mgr/pubsub' )

const persistence = require( './db-persistence' )

module.exports = {
  init,
  setReplQuorum,

  count,
  find,
  findDocs,
  findOneDoc,
  findByPkKey,
  findByIdxQry,
  getAllDoc,
  creFindMsg,
  creDocsFoundResponse

}

let REPLICATION_QUORUM = 2
let nodeMgr = null

function init( theNodeMgr ) {
  nodeMgr = theNodeMgr
}

function setReplQuorum( quorum ) {
  REPLICATION_QUORUM = quorum
}

// ============================================================================
/*  
  Supported Queries:

  Find by primary key PK = ['x','y']: 
    { x: val1 , y: val2 } 
*/


async function find( dbName, collName, query, options = {} ) {
  log.debug( 'find',  dbName, collName, query )
  let req = {
    db      : dbName,
    coll    : collName,
    dt      : Date.now(),
    txnId   : helper.getTxnId( 'FND' ),
    options : options
  }
  let result = await findDocs( req, query )
  // log.info( 'find >>>>>>>>>>>>>>>>>>>>>>', result )
  return result
}


async function count( dbName, collName, query ) {
  let req = {
    db      : dbName,
    coll    : collName,
    dt      : Date.now(),
    txnId   : helper.getTxnId( 'FND' ),
    options : options
  }
  let result = await findDocs( req, query )
  // log.info( 'find >>>>>>>>>>>>>>>>>>>>>>', result )
  return result
}


async function findDocs( r, qry ) {
  try {
    let query = ( typeof qry  == 'string' ? JSON.parse( qry ) : qry )
    let findRequest = await creFindMsg( r, query )
    if ( r.db != 'admin' )
      log.info( r.txnId, 'findRequest',  r.db, r.coll, query, findRequest.op )
    if ( findRequest._error ) { return findRequest }
    // if ( findRequest._error ) { 
    //   res.status( st.BAD_REQUEST ).send( findRequest ) 
    // }

    let quorum = undefined
    if ( r.options?.optimize == 'only master nodes' ) {
      quorum = nodeMgr.getClusterSize()
    }

    // if ( r.db != 'admin' ) { log.info( r.txnId, 'findRequest',  r.db, r.coll,  findRequest.op, quorum ) }
    
    if ( findRequest.op == 'find all doc' ) {
      pubsub.sendRequestAllNodes( r.txnId, findRequest )
      quorum = nodeMgr.getClusterSize()
    } else if ( findRequest.op == 'find by PK' ) {
      pubsub.sendRequest( r.txnId, findRequest.token, findRequest )
    } else if ( findRequest.op == 'find by IDX' ) {
      pubsub.sendRequestAllNodes( r.txnId, findRequest )
      quorum = nodeMgr.getClusterSize()
    } else if ( findRequest.op == 'find full scan' ) {
      pubsub.sendRequestAllNodes( r.txnId, findRequest )
      quorum = nodeMgr.getClusterSize()
    }

    let coll = await persistence.getColl(r.db, r.coll )
    if ( coll.masterData ) { // data is on all nodes, so simple quorum is enough
      quorum = undefined
    }

    // if ( r.db != 'admin' ) { log.info( r.txnId, 'findRequest',  r.db, r.coll,  findRequest.op, quorum ) }

    let result = await pubsub.getReplies( r.txnId, quorum )
    // log.info( 'findRequest >>>>>>>>>>>>>>>>>>>>> docIds', r, result )
    let response = creDocsFoundResponse( r.txnId, result, r.options )
    // log.info( 'findRequest >>>>>>>>>>>>>>>>>>>>> docIds', r, response.docIds )

    return response
  } catch ( exc ) {
    return { _error: 'Find error: '+exc.message  }
  }
}

async function findOneDoc( r, filter ) {
  let rClone = JSON.parse( JSON.stringify( r ) )
  if ( ! rClone.txnId ) { rClone.txnId = helper.randomChar( 10 ) }
  rClone.txnId = rClone.txnId + '.FND'
  let findResult = await findDocs( rClone, filter ) 
  // log.info( '>>>>>>>>>>> findResult', findResult)
  if ( findResult._ok && findResult.dataLength == 1 ) {
    return { _ok: true, doc: findResult.data[ 0 ] }
  } else {
    return { _error: 'Documents matching: '+findResult.dataLength  }
  }
}

async function findByPkKey( dbName, collName, colSpec, query, options = {} ) {
  try {
  log.info( 'findByPkKey', dbName, collName, colSpec, query  )

  if ( ! colSpec ) { return { _error: 'Collectiom not found'} }
  let pkQryStr = helper.getQrynKeyStr( query, colSpec.pk )
  let docId    = await helper.getKeyHash( pkQryStr )
  if ( collName != 'api-metrics') 
    log.info( 'docId', docId )
  let docToken = helper.extractToken( docId )
  let nodes    = nodeMgr.getNodes( docToken )

  log.debug( 'find', docId, nodes )
  let getDocFromNodes = []

  if ( options.readConcern && options.readConcern == 'available' ) {
    log.debug( 'find readConcern available >>>>>>>>>>>>>', nodes, ownNodeAddr() )
    for ( let dbTarget of nodes ) { // try to avoid to ask net for quorum, but may loose consistency
      if ( dbTarget.node == ownNodeAddr() ) {
        log.debug( 'find readConcern available >>>>>>>>>>>>>', dbTarget )
        let docResult = await getDocById( dbName, collName, docId )
        if ( ! docResult._error ) {
          return { 
            doc : docResult.doc, 
            _ok  : true,
            quorum : 1
          }
        }
      }
    }
  } 

  for ( let dbTarget of nodes ) {
    if ( dbTarget.node == ownNodeAddr() ) { // it's me
      log.debug( 'find', docId, 'get doc locally')
      getDocFromNodes.push(
        persistence.getDocById( dbName, collName, docId, options )
      )
    } else {
      let docURI = '/db/'+ dbName +'/'+ collName +'/'+ docId 
      log.warm( 'find >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', docId, 'get doc from', docURI )
      getDocFromNodes.push(
        api.get( dbTarget.node, docURI, { withTxnId: true } )
      )
    }
  }
  let docResults = await Promise.allSettled( getDocFromNodes )
  // log.debug( 'docResults', JSON.stringify( docResults, null, ' ' ) )


  // check quorum and prepare result
  let docHashStat = { }
  for ( let resultVal of docResults ) {
    genResultCheckHash( resultVal, docHashStat )
  }
  log.debug( 'docResults docHashStat', REPLICATION_QUORUM, docHashStat )

  let result = {}
  for ( let docHash in docHashStat ) {
    let stat = docHashStat[ docHash ]
    if ( stat.cnt >= REPLICATION_QUORUM ) {
      // TODO, if ( stat.cnt != cfg.DATA_REPLICATION ) { syncReplication }
      result = { 
        doc : stat.doc, 
        _ok  : true,
        quorum : stat.cnt
      }
    } else if ( stat._error ) {
      result = { 
        _error  : stat._error,
      }
    }
  }
  return result
    
  } catch ( exc ) {
      log.error( 'findByPkKey', exc )
      return { _error: exc.message }
  }
}

// ============================================================================

async function getAllDoc( txnId, dbName, collName, query, proj, options={}, isIdxQry ) {
  try {
    log.debug( txnId, 'getAllDoc', dbName, collName, (query? JSON.stringify(query):'-'), proj, options )
    let limit = options.limit || 1000

    let idArr = null
    if ( isIdxQry ) {
      let collIdx = await persistence.geCollIdx( dbName, collName )
      if ( collIdx._ok ) {
        let idxScan = await persistence.findDocCandidates( txnId, dbName, collName, query,  collIdx.idx )
        if ( idxScan ) {
          if ( idxScan._error ) { return idxScan }
          if ( idxScan.pureIdx ) {
            log.debug( txnId, 'getAllDoc pure index query result', query,  idxScan.ids.length )
            let result = {
              doc   : [],
              docId : idxScan.ids,
              _ok   : true
            }
            if ( ! options.idsOnly ) {
              let cnt = 0
              for ( let docId of result.docId ) {
                try {
                  const data = await persistence.getDocById( dbName, collName, docId )  // this uses doc cache
                  if ( ! data._error ) {
                    if ( proj && Array.isArray( proj ) ) {
                      result.doc.push( projectDoc( data.doc, proj ) )
                    } else {
                      result.doc.push( data.doc )
                    }
                    cnt ++
                  }
                } catch ( exc ) { log.warn( txnId, 'getAllDoc',docId, exc.message )}
                if ( cnt == limit ) { break }
              }
            }
            // log.info( result )
            return result
          } else if ( ! idxScan.ids.includes(  '_ALL_' ) ) {
            idArr = idxScan.ids
            log.debug( txnId, 'getAllDoc idx result, but needs more criteria', query, idArr.length )
          }
        }
      }
      log.debug( txnId, 'getAllDoc found', query, idArr?.length )
    } 
    if ( ! idArr  ) {
      idArr = await persistence.getAllDocIds( txnId, dbName, collName, options )
    }
    if ( idArr._error ) { return idArr }
    log.debug( txnId, 'getAllDoc  idArr.length ', idArr?.length )
    let result = {
      doc : [],
      docId : []
    }
    if ( options.idsOnly ) { 
      result.idsOnly = true
    }
    log.debug( txnId, 'getAllDoc limit ', limit )
    let cnt = 0
    for ( let id of idArr ) {

      if ( options.idsOnly  && ! query ) { 
        result.docId.push( id )
        continue // don't need to look into doc at all, e.g. fo counting docs
      }

      const data = await persistence.getDocById( dbName, collName, id )  // this uses doc cache
      log.debug( 'getAllDoc data', id, data, query )
      if ( ! data._error && matchesQuery( data.doc, query ) ) {
        // log.info( 'getAllDoc matches', id )
        if ( ! options.idsOnly ) {
          if ( proj && Array.isArray( proj ) ) {
            result.doc.push( projectDoc( data.doc, proj ) )
          } else {
            result.doc.push( data.doc )
          }
        }
        result.docId.push( id )
        cnt ++
        if ( cnt >= limit ) { break }

      }
    }
    result._ok = true
    log.debug( txnId, 'getAllDoc cnt', result.doc.length )
    return result
  } catch ( exc ) { 
    return errorMsg( txnId + ' getAllDoc', exc )
  }
}


function projectDoc( doc, proj ) {
  log.info( 'project doc', doc, proj )
  let result = {
    _id : doc._id
  }
  for ( let prop of proj ) {
    if ( prop.indexOf('.') > 0 ) {
      let dot = prop.indexOf('.')
      let propBase = prop.substring( 0, dot )
      let subProp  = prop.substring( dot + 1 )
      // log.info( 'subprop', dot, propBase, subProp )
      if ( doc[ propBase ] ) {
        result[ propBase ] = projectDoc( doc[ propBase ], subProp ) 
      }
    } else {
      result[ prop ] = doc[ prop ]
    }
  }
  return result
}

function matchesQuery( doc, query )  {
  log.debug( 'matchesQuery', doc, query  )
  try {
    if ( ! query || query == {} ) { return true }

    for ( let qKey in query ) {
      let qFld = query[ qKey ]

      if ( qKey == '$and') {

        for ( let oneCheck of qFld ) { 
          let oneResult = matchesQuery( doc, oneCheck )
          if ( oneResult == false ) { return false }
        }
        return true
      
      } else if ( qKey == '$not' ) {

        for ( let oneCheck of qFld ) { 
          let oneResult = matchesQuery( doc, oneCheck )
          if ( oneResult == false ) { return true }
        }
        return false

      } else if ( qKey == '$nor' ) {

        for ( let oneCheck of qFld ) { 
          let oneResult = matchesQuery( doc, oneCheck )
          if ( oneResult == true ) { return false }
        }
        return true

      } else if ( qKey ==  '$or' ) {
        
        for ( let oneCheck of qFld ) { 
          let oneResult = matchesQuery( doc, oneCheck ) 
          if ( oneResult == true ) { return true }
        }
        return false

      } else 

        // simple field comparison ?
      if ( typeof qFld === 'string' || typeof  qFld === 'number' || typeof  qFld === 'boolean' ) {

        // TODO sub docs
        let docVal = getSubDoc( doc, qKey )
        if ( ! docVal ) {
          return false
        }
        if (docVal != qFld ) {
          return false
        }

      } else 
      
      if ( typeof  qFld === 'object' ) {
        let docVal = getSubDoc( doc, qKey )
        log.debug( 'matchesQuery object', qKey, docVal, qFld )

        let eval = helper.evalQueryExpr( docVal, qFld )
        if ( eval._error ) {
          return false // TODO .. return error
        }
        return  eval.isIn
      } else { 
        log.warn( 'matchesQuery not supported', qKey, qFld )
        return false
      }

    }
  } catch ( exc ) { log.warn( 'matchesQuery', exc ) }
  return true // all checks done
}


function getSubDoc( doc, key ) {
  while ( key.indexOf('.') > 0 ) {
    let firstFld = key.substring( 0,  key.indexOf('.') )
    if ( doc[ firstFld ] != undefined ) {
      // log.info( 'getSubDoc', doc[ firstFld ], key.substring( key.indexOf('.') + 1) )
      return getSubDoc(  doc[ firstFld ], key.substring( key.indexOf('.') + 1) )
    } else { return null }
  } 
  return doc[ key ]
}

// ============================================================================


async function findByIdxQry( dbName, collName, colSpec, query, options ) {
  log.warn( 'findByIdxQry', 'TODO implement findByIdxQry')
  if ( ! colSpec ) { return { _error: 'Collection not found'} }

  let idxQuery   = { }
  let otherQuery = { }
  for ( const idxField of colSpec.idx ) {
    if ( query[ idxField ] || query[ idxField ] === false ) {
      idxQuery[ idxField ] = query[ idxField ] 
      let docId = await askPodForIdxDoc( idxField, query[ idxField ] )

    } else {
      let otherQuery = { }
      otherQuery[ idxField ] = query[ idxField ] 
    }
  }

  result = { 
    _error  : 'TODO'
  }
  return result
}



async function creFindMsg( r, query ) {
  log.debug( r.txnId, 'DB creFindMsg', r.db, r.coll, query ) 
  let findRequest = {
    op    : 'find full scan',
    txnId : r.txnId,
    db    : r.db,
    col   : r.coll,
    qry   : query,
    opt   : r.options,
    proj  : r.proj
  }
  log.debug( r.txnId, 'DB creFindMsg', findRequest ) 
  let colSpec = await persistence.getCollSpec( r.db, r.coll )
  if ( ! colSpec ) { return { _error: 'Collection not found'} }
  log.debug( r.txnId, 'DB find colSpec', query, colSpec )

  if ( ! query || query == {} ) {

    log.debug( r.txnId, 'DB find', 'get all docs' )
    findRequest.op = 'find all doc'
    return findRequest

  } else if ( helper.isPkQuery( query, colSpec ) ) {

    let docId = await helper.getPkHash(  r.db, r.coll, query, colSpec.pk )
    findRequest.token  = helper.extractToken( docId )
    findRequest.docId = docId
    log.debug( r.txnId, 'DB find', 'by PK', docId, findRequest.token )
    findRequest.op = 'find by PK'
    return findRequest

  } else if ( helper.isIdxQuery( query, colSpec ) ) {
    
    log.debug( r.txnId, 'DB find', 'by Idx' )
    findRequest.op = 'find by IDX'
    // result = await findByIdxQry( dbName, collName, colSpec, query, options )
    return findRequest

  } else {

    log.debug( r.txnId, 'DB find', 'full scan')
    findRequest.op = 'find full scan'
    return findRequest

  }
}


function creDocsFoundResponse( txnId, results, options ) {

  let response = { 
    _ok : true
  }

  if ( ! results._ok ) {
    response._ok    = false
    response._error = results._error
  }

  if ( ! results.replyMsg ) {
    return results
  }

  response._okCnt     = 0
  response._nokCnt    = 0
  response.docIds     = []
  response.data       = []
  response.dataLength = 0 

  if ( results.resultIsArray ) {

      for ( let msg of results.replyMsg ) {
        let data = ( msg.content ? JSON.parse( msg.content.toString() ) : msg )
        if ( options  &&  options.idsOnly ) {
            response.idsOnly = true
          if ( data._ok && data.docId ) {
            for ( let id of data.docId ) {
              if ( ! response.docIds.includes( id ) ) {
                response.docIds.push( id )
                response.dataLength ++  
              }
            }
            response._okCnt ++
          } else {
            response._nokCnt ++
          }
        } else {
          // log.info( 'creDocsFoundResponse >>>>>>>>>>>>>>>>>  msg.properties.headers' , msg.properties.headers )
          if ( data._ok && data?.doc ) {
            // log.info( creDocsFoundResponse >>>>>>>>>>>>>>>>> docs' , data?.doc )
            for ( let doc of data.doc ) {
              // log.info( 'creDocsFoundResponse >>>>>>>>>>>>>>>>> a doc' , doc._id )
              if ( ! response.docIds.includes( doc._id )) {
                response.docIds.push( doc._id )
                response.data.push( doc )
                response.dataLength ++
              }
            }
            response._okCnt ++
          } else {
            response._nokCnt ++
          }
        }
      }

  } else {

      let ids = []
      for ( let msg of results.replyMsg ) {
        // if ( txnId.startsWith('FND') ) log.info( 'creDocsFoundResponse', msg.content.toString() )
        try {
          let data = ( msg.content ? JSON.parse( msg.content.toString() ) : msg )
          // if ( txnId.startsWith('FND') ) log.info( 'creDocsFoundResponse', data )
          if ( data && ! data._error  &&  ! ids.includes( data.doc._id ) ) {
            ids.push( data.doc._id )
            response.data.push( data.doc )
            response.dataLength ++
          }
        } catch ( exc ) { log.warn( 'creDocsFoundResponse exc', exc.message, msg ) }
       
      }
      response.docIds = ids
  }

  return response
}



async function askPodForIdxDoc( idxField, idxVal ) {
  let idxToken = idxVal.charCodeAt(0).toString(16)
  // TODO
}



function genResultCheckHash( resultVal, docHashStat ) {
  if ( resultVal.value  &&  resultVal.status == 'fulfilled' ) {
    let docCopy = resultVal.value.doc 
    let docHash =  hash( docCopy ) // should be enough here
    if ( ! docHashStat[ docHash ] ) { docHashStat[ docHash ] = { cnt: 0, errcnt: 0, node: [], doc: null } }
    if ( resultVal.value._error ) {
      docHashStat[ docHash ].errcnt ++
      docHashStat[ docHash ]._error = resultVal.value._error
    } else {
      docHashStat[ docHash ].cnt ++
      docHashStat[ docHash ].node.push( resultVal.value.node )  
      docHashStat[ docHash ].doc = docCopy   
    }
  } else { 
    log.warn( 'find docResults', resultVal.status  )
    if ( ! docHashStat[ docHash ] ) { docHashStat[ docHash ] = { cnt: 0, errcnt: 1, node: [], doc: null } }
    docHashStat[ docHash ]._error = resultVal.status 
    /* TODO: oups, whoch node was that?? */ 
  }
}



function errorMsg( fnName, exc ) {
  log.fatal( fnName, exc )
  return { _error : ''+exc.message }
}
