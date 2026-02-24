const cfgHlp = require( '../helper/config' )
const log   = require( '../helper/logger' ).log
const fs    = require( 'fs' )
const { mkdir, writeFile, readFile, rm, rmdir, stat, readdir, open, opendir } = require( 'node:fs/promises' )
const dbFile  = require( './db-file' )
const helper  = require( './db-helper' )


module.exports = {
  init, 
  findDocCandidates
}

// ============================================================================

let cfg = {
  DATA_DIR: './db/'
}

function init( configParams ) {
  // log.info( 'persistence.init', configParams )
  cfgHlp.setConfig( cfg, configParams )
  // log.info( 'persistence.init', cfg )
}

// ============================================================================
// Example:
//   myColl.find( {
//     $and: [
//        { $and: [ 
//          { address.zip : { $ge : 40000 } }, 
//          { address.zip : { $lt : 50000 } } 
//        ] },
//        { $or: [ 
//          { status: 'Premium' }, 
//          { revenue : { $gt : 100000 } } 
//        ] }
//      ]
//    })
async function findDocCandidates( txnId, dbName, collName, query, idx ) {
  try {
    log.info( txnId, 'findDocCandidates', 'query', dbName, collName, query, idx  )
    if ( ! query || query == {} ) { return null }

    let matchDocIDs = [ '_ALL_' ]
    let pureIdx = true
    for ( let qKey in query ) {
      let qFld = query[ qKey ]

      if ( qKey == '$and') {
        // $and: [ idx, other ] -> OK

        for ( let oneCheck of qFld ) { 
          let oneResult = await findDocCandidates( txnId, dbName, collName, oneCheck, idx )
          if ( oneResult._error ) { return { _error: 'Query error: '+oneCheck } }
          if ( ! oneResult.pureIdx ) { pureIdx = false }
          log.info( txnId, 'findDocCandidates and result', oneResult )
          if ( matchDocIDs.includes( '_ALL_') ) {
            matchDocIDs = oneResult.ids
          } else 
          if ( ! oneResult.ids.includes( '_ALL_' ) ) {
            let andMatch = []
            for ( let id of oneResult.ids ) {
              if ( matchDocIDs.includes( id ) ) { // is in both
                andMatch.push( id )
              }
            }
            matchDocIDs = andMatch
           }
        }
        return { pureIdx: pureIdx, ids: matchDocIDs }
      
      } else if ( qKey == '$not' ) {
        // $not: [ idx, other ] -> OK
        
        let notIDs = []
        for ( let oneCheck of qFld ) { 
          // log.info( 'Query $not:', oneCheck )
          let oneResult = await findDocCandidates( txnId, dbName, collName, oneCheck, idx )
          if ( oneResult  ) { 
            notIDs = notIDs.concat( oneResult.ids )
            if ( ! oneResult.pureIdx ) { pureIdx = false }
          }
        }
        matchDocIDs = await invertDocsFound( txnId, dbName, collName, notIDs )
        return { pureIdx: pureIdx, ids: matchDocIDs }

      } else if ( qKey == '$nor' ) {
        // $nor: [ idx, other ] -> OK

        // first or them all
        let orIDs = []
        let oneResult = await findDocCandidates( txnId, dbName, collName, oneCheck, idx ) 
        if ( oneResult._error ) { return { _error: 'Query error: '+oneCheck } }
        if ( ! oneResult.pureIdx ) { pureIdx = false }
        if ( oneResult.ids.includes( '_ALL_' ) ) {
          orIDs = oneResult.ids
        } else {
          orIDs = orIDs.concat( oneResult.ids )
        }
        // and not invert it
        matchDocIDs = await invertDocsFound( txnId, dbName, collName, orIDs )
        return { pureIdx: pureIdx, ids: matchDocIDs }

      } else if ( qKey ==  '$or' ) {
        // $or: [ idx, other ] -> NOK

        for ( let oneCheck of qFld ) { 
          let oneResult = await findDocCandidates( txnId, dbName, collName, oneCheck, idx ) 
          if ( oneResult._error ) { return { _error: 'Query error: '+oneCheck } }
          if ( ! oneResult.pureIdx ) { pureIdx = false }
          if ( oneResult.ids.includes( '_ALL_' ) ) {
            return { pureIdx: false, ids: '_ALL_' }
          } else {
            matchDocIDs = matchDocIDs.concat( oneResult.ids )
          }
        }
        return { pureIdx: pureIdx, ids: matchDocIDs }

      } else 

        if ( ! idx[ qKey ] ) {
          return { pureIdx: false, ids: ['_ALL_'] } // not an index query
        }

        let qryVal = qFld
        let maxLen = 100
        if ( idx[ qKey ]?.msbLen && Number.isInteger( idx[ qKey ].msbLen ) ) {
          maxLen = idx[ qKey ].msbLen 
        }

        switch ( typeof qFld ) {
          case 'number': qryVal = ''+qFld; break
          case 'string': qryVal =  qFld.substring( 0, maxLen ); break
          case 'object': qryVal = JSON.stringify( qFld ).substring(0,maxLen ); break
          case 'boolean': qryVal = '_'+qFld; break
          case 'undefined': qryVal = '_undefined_'; break
          case 'bigint': qryVal = qFld.toString(16); break
        }


      // simple field comparison ?
      if ( typeof qFld === 'string' || typeof  qFld === 'number' || typeof  qFld === 'boolean' ) {

        log.info( txnId, 'findDocCandidates', 'simple index query', qKey, qryVal )
        let ids = await getDocsEqIndex( txnId, dbName, collName, qKey, qryVal )
        return { pureIdx: true, ids: ids }

      } else 
      
      if ( typeof  qFld === 'object' ) {

        log.info( txnId, 'findDocCandidates', 'condition index query', qKey, qFld )
        let ids = await getDocsCompIndex( txnId, dbName, collName, qKey, qFld )
        if ( ! ids ) { return { _error: 'Error in query: ' +  qKey +' ' + qryVal }}
        if ( ids._error ) { return { _error: 'Error in query: ' + idx._error +' ('+  qKey +' ' + qryVal+')' }}
        return { pureIdx: true, ids: ids }


      } else { 
        log.warn( 'findDocCandidates not supported', qKey, qFld )
        return { _error: 'Query not supported: '+qKey+' '+qFld }
      }

    }
    return matchDocIDs
  } catch ( exc ) { log.warn( 'findDocCandidates', exc ) }
  return  { ids:  ['_ALL_']  }
}

// ============================================================================

async function getDocsEqIndex( txnId, dbName, collName, qKey, val ) {
  let idxDir = dbFile.collPath( dbName, collName ) +'/idx/'+ qKey 
  let docIDs = []
  let fileArr = fs.readdirSync( idxDir, { withFileTypes: true } )
  for ( let x of fileArr ) {
    // log.info( txnId,'getDocsByIndex load', x.name )
    if ( x.name.endsWith( '.json' ) ) {
      let idx = JSON.parse( await readFile( idxDir +'/'+ x.name ) )
      if ( idx[ val ] ) {
        log.info( txnId,'getDocsByIndex match', x.name, val, idx[ val ].length )
        docIDs = docIDs.concat( idx[ val ] )
      }
    }
  }
  return docIDs
}

async function invertDocsFound( txnId, dbName, collName, docIds ) {
  let allDocIDs = await dbFile.getAllDocIds( txnId, dbName, collName )
  // log.info( 'allDocIDs', allDocIDs)
  let result = []
  for ( let id of allDocIDs ) {
    if ( ! docIds.includes( id ) ) {
      result.push( id )
    }
  }
  return result
}

// ----------------------------------------------------------------------------

async function getDocsCompIndex( txnId, dbName, collName, qKey, expr ) {
  let idxDir = dbFile.collPath( dbName, collName ) +'/idx/'+ qKey 
  let docIDs = []
  let fileArr = fs.readdirSync( idxDir, { withFileTypes: true } )
  for ( let x of fileArr ) {
    log.debug( txnId, 'getDocsCompIndex load', x.name )
    if ( x.name.endsWith( '.json' ) ) {
      let idx = JSON.parse( await readFile( idxDir +'/'+ x.name ) )

      for ( let idxVal in idx ) {

        let eval = helper.evalQueryExpr( idxVal, expr )
        if ( eval._error ) {
          return eval
        }

        if ( eval.isIn === true) {
          log.debug( txnId, 'getDocsCompIndex expr ok', idxVal, expr )
          docIDs = docIDs.concat( idx[ idxVal ] )
        }
      }
    }
  }
  return docIDs
}

// ============================================================================
// helper

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

async function loadIndexDef( dbName, collName ) {
  let idxDef = {}
  try {
    let collIdxFile = dbFile.collPath( dbName, collName ) + '/idx/idx.json'
    log.info( 'loadIndexDef', collIdxFile )
    if ( await dbFile.fileExists( collIdxFile ) ) {
      idxDef = JSON.parse( await readFile( collIdxFile ) )
    }
  } catch ( exc ) { log.error( 'loadIndexDef', exc ) }
  return idxDef
}
