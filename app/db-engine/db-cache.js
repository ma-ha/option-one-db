const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log

module.exports = {
  init,
  addToCache,
  getDocFrmCache,
  rmFrmCache
}

// ============================================================================

let cfg = {
  MAX_CACHE_MB : 10,
}

async function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================
// in memory cache 

let docCache = {}
let docCacheSize = 0
let MAX_CACHE_SIZE = cfg.MAX_CACHE_MB * 1000000

async function addToCache( dbName, collName, docId, doc ) {
    try {
    if ( ! docCache[ dbName ] ) { docCache[ dbName ] = {} }
    if ( ! docCache[ dbName ][ collName ] ) { docCache[ dbName ][ collName ] = { _colSz : 0, _cnt : 0  } }
    let dataSize = calcMemSize( doc )
    docCache[ dbName ][ collName ][ docId ] = {
      dt  : Date.now(),
      doc : doc,
      sz  : dataSize
    }
    docCache[ dbName ][ collName ]._colSz += dataSize
    docCache[ dbName ][ collName ]._cnt ++
    docCacheSize += dataSize
    log.debug( 'ADD-CACHED', dbName, collName, docId, dataSize, docCacheSize )
    if ( docCacheSize > MAX_CACHE_SIZE ) {
      cleanupCache()
    }
  } catch ( exc ) { log.error( 'cleanupCache', exc ) }
}

let cleanupInProcess = false

setInterval( cleanupCache, 10000 )

function cleanupCache() {
  try {
    if ( cleanupInProcess ) { return }
    if ( docCacheSize < MAX_CACHE_SIZE ) { return }
    cleanupInProcess = true

    let dbCols = [ ]
    for ( let dbName in docCache ) {
      for ( let collName in docCache[ dbName ] ) {
        if ( docCache[ dbName ][ collName ]._cnt == 0 ) { continue }
        dbCols.push({ 
          db   : dbName, 
          coll : collName, 
          sz   : docCache[ dbName ][ collName ]._colSz,
          cnt  : docCache[ dbName ][ collName ]._cnt
        })
      }
    }
    dbCols.sort( ( a, b ) => { return b.sz - a.sz } )
    log.debug( 'cleanupCache', dbCols )

    for ( dbColl of dbCols ) {
      if ( dbColl.db == 'admin' ) { continue }
      let oldestDocs = []
      for ( let docId in docCache[ dbColl.db ][ dbColl.coll ] ) {
        if ( docId == '_colSz' || docId == '_cnt' ) { continue }
        oldestDocs.push({ id: docId, dt: docCache[ dbColl.db ][ dbColl.coll ][ docId ].dt })
      }
      oldestDocs.sort( ( a, b ) => { a.dt - b.dt })
      // clean up 10%
      let tenPercent = Math.ceil( oldestDocs.length * 0.1 )
      log.debug( 'cleanupCache del', tenPercent, dbColl )
      for ( let i = 0; i < tenPercent; i++ ) {
        rmFrmCache( dbColl.db,  dbColl.coll,  oldestDocs[ i ].id )
      }
    }

    cleanupInProcess = false
  } catch ( exc ) { log.error( 'cleanupCache', exc ) }
}

function getDocFrmCache( dbName, collName, docId ) {
  try {
    if ( ! docCache[ dbName ] ) { return null }
    if ( ! docCache[ dbName ][ collName ] ) { return null }
    let cached =  docCache[ dbName ][ collName ][ docId ]
    if ( cached ) { 
      docCache[ dbName ][ collName ][ docId ].dt = Date.now()
      log.debug( 'FRM-CACHED', dbName, collName, docId )
      return cached.doc
    }
  } catch ( exc ) { log.error( 'cleanupCache', exc ) }
  return null
}

function rmFrmCache( dbName, collName, docId ) {
  try {
    if ( ! docCache[ dbName ] ) { return }
    if ( ! docCache[ dbName ][ collName ] ) { return }
    let cached =  docCache[ dbName ][ collName ][ docId ]
    if ( cached ) {
      log.debug( 'DEL-CACHED', dbName, collName, docId )
      docCache[ dbName ][ collName ]._colSz -= cached.sz
      docCacheSize -= cached.sz
      docCache[ dbName ][ collName ]._cnt --
      delete docCache[ dbName ][ collName ][ docId ]
    }
  } catch ( exc ) { log.error( 'cleanupCache', exc ) }
}

function calcMemSize( data ) {
  if ( Buffer.isBuffer( data ) ) { return data.length }
  switch ( typeof data ) {
    case 'string':
      return data.length * 2 // good enough
    case 'boolean':
      return 4
    case 'number':
      return 8
    case 'object':
      let sum = 0
      if ( Array.isArray( data ) ) {
        for ( let elem of data ) {
          sum += calcMemSize( elem )
        }
      } else {
        for ( let elem in data ) {
          sum += calcMemSize( data[ elem ] )
        }
      }
      return sum
      default:
      return 0
  }
}