const log     = require( '../helper/logger' ).log

module.exports = {
  setTokenLength,
  toggleMeasurePerfOn,

  adminDbUserSpec,
  adminDbSessionSpec,
  newAdminUser,
  newMochaTestUser,
  
  demoCollSpec,
  demoCollRec,

  isPkQuery,
  isIdxQuery,
  evalQueryExpr,

  extractToken,
  getPkHash,
  genKeyStr,
  getQrynKeyStr,
  getKeyHash,
  getTokenFromStr,

  getTxnId,
  randomChar,
  randomHex,

  getKeyVal,

  dbgStart,
  dbgStep,
  dbgEnd,
  dbgPrint
}

let TKN_LEN = 1

function setTokenLength( tokenLen ) {
  TKN_LEN = tokenLen
}

let MEASURE_PERF = false
function toggleMeasurePerfOn() {
  MEASURE_PERF = true
}


function adminDbUserSpec() {
  return { 
    pk : ['user'],
    index : { email: { type:'String', unique: true } },
    masterData : true
  }
}

function adminDbSessionSpec() {
  return  { 
    pk : ['token'],
    masterData : true
  }
}

async function newAdminUser() {
  // TODO let user = 'admin_'+ randomChar( 2 )
  let user = 'admin'
  let pwd  = genPasswd()

  const { createHash } = await import('node:crypto')
  const hash = createHash('sha256')
  hash.update( user + pwd )
  let passwordHash = hash.digest('hex')

  let buf = Buffer.from( user )

  let admin = {
    // _id      : buf.toString('hex'),
    user     : user,
    email    : 'admin@ad.min',
    passwordHash      : passwordHash,
    passwordTimestamp : Date.now(),
    autz     : { 'admin' : 'w', '*' : 'w' } 
  }
  log.info( 
    '================================' +
    '\n Admin User: '+ user+'   Password: '+ pwd + '       '+
    '\n please change the password ASAP '+
    '\n================================' 
  )
  return admin
}

let user = 'mocha'
async function newMochaTestUser(  pwd ) {
  // return { _id: 'mocha', password:'test', autz: { 'mocha-test-db': 'w'} }

  // let user = 'mocha'
  // let pwd  = 'test'

  const { createHash } = await import('node:crypto')
  const hash = createHash('sha256')
  hash.update( pwd )
  let passwordHash = hash.digest('hex')

  let buf = Buffer.from( user )

  let mocha = {
    // _id      : buf.toString('hex'),
    user     : user,
    email    : 'mocha@test.mock',
    passwordHash      : passwordHash,
    passwordTimestamp : Date.now(),
    autz     : { 'mocha-test-db': 'w' }
  }
  return mocha
}

function demoCollSpec() {
  return  { 
    pk : [ 'email' ],
    i  : { country: {} }
  }
}

function demoCollRec() {
  return { name : 'Moe', email : 'moe@test.net', country : 'Germany' }
}


function randomChar( len ) {
  var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  var token =''
  for ( var i = 0; i < len; i++ ) {
    var iRnd = Math.floor( Math.random() * chrs.length )
    token += chrs.substring( iRnd, iRnd+1 )
  }
  return token
}


function genPasswd() {
  var chrs1 = "abcdefghijklmnopqrstuvwxyz"
  var chrs2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  var chrs3 = "0123456789"
  var chrs4 = "=-+_!"
  var token =''
  for ( var i = 0; i < 12; i++ ) {
    let t =  Math.random() * 10
    if ( t < 3 ) {
      var iRnd = Math.floor( Math.random() * chrs1.length )
      token += chrs1[ iRnd ]  
    } else if ( t < 6 ) {
      var iRnd = Math.floor( Math.random() * chrs2.length )
      token += chrs2[ iRnd ]  
    } else if ( t < 8 ) {
      var iRnd = Math.floor( Math.random() * chrs3.length )
      token += chrs3[ iRnd ]  
    } else {
      var iRnd = Math.floor( Math.random() * chrs4.length )
      token += chrs4[ iRnd ]  
    }
  }
  return token
}

//=============================================================================

function isPkQuery( query, colSpec ) {
  log.debug( 'isPkQuery', query, colSpec )
  let allPK  = true
  for ( const pKey of colSpec.pk ) {
    if ( ! query[ pKey ] ) { allPK = false }
    if ( typeof   query[ pKey ] === 'object'  ) { allPK = false }
  }
  log.debug( 'isPkQuery', allPK )
  return allPK
}

function isIdxQuery( query, colSpec ) {
  log.debug( 'HELPER isIdxQuery', query, colSpec )
  let isIdx  = false
  if ( colSpec.idx ) {
    for ( const q in query ) {
      if ( [ '$and', '$or', '$nor', '$not' ].includes( q ) ) {
        for ( let cond of query[ q ] ) {
          isIdx = isIdxQuery( cond, colSpec )
          if ( isIdx ) { return true}
        }
      } else if ( colSpec.idx[ q ] ) {
        log.debug( 'HELPER isIdxQuery >>', query, q )
        isIdx = true
      }
    }
  }
  return isIdx
}

//=============================================================================

function evalQueryExpr( dtaVal, expression ) {
  log.info( 'evalQueryExpr', dtaVal, expression )
  let isIn = false

  for ( let comparator in expression ) {
    let compVal = expression[ comparator ]

    if ( comparator == '$lt' ) {
            
      if ( dtaVal <  compVal ) { isIn = true } else { isIn = false }

    } else  if ( comparator == '$le'  ) {
    
      if ( dtaVal <= compVal ) { isIn = true } else { isIn = false }

    } else  if ( comparator == '$gt' ) {
    
      if ( dtaVal > compVal ) { isIn = true } else { isIn = false }

    } else  if ( comparator == '$ge') {
    
      if ( dtaVal >= compVal ) { isIn = true } else { isIn = false }

    } else  if ( comparator == '$eq') {
    
      if ( dtaVal == compVal ) { isIn = true } else { isIn = false }

    } else  if ( comparator == '$ne') {
    
      if ( dtaVal != compVal ) { isIn = true } else { isIn = false }

    } else if ( comparator == '$in' ) {

      if (  Array.isArray( compVal ) ) {
        let cmpArr = []
        for ( let x of compVal ) { cmpArr.push( x+'' ) }
        if ( cmpArr.includes( dtaVal+'' ) ) { isIn = true } else { isIn = false }
      } else { return false }

    } else if ( comparator == '$nin' ) {
      
      // log.info( '$nin', dtaVal, typeof dtaVal, compVal )
      if (  Array.isArray( compVal ) ) {
        let cmpArr = []
        for ( let x of compVal ) { cmpArr.push( x+'' ) }
        if ( cmpArr.includes( dtaVal+'' ) ) { isIn = false } else { isIn = true }
      } else { return false }

    } else if ( comparator == '$like') {
      
      // log.info( 'like object', dtaVal, compVal )
      if ( typeof compVal === 'string'  ) {
        if ( dtaVal.indexOf( compVal ) >= 0 ) { isIn = true } else { isIn = false }
      } else { return false }

    } else {
      log.warn( 'matchesQuery obj not supported', comparator, compVal )
      return false
    } 
  }
  return { isIn: isIn }
}
//=============================================================================

function extractToken( pKeyHash ) {
  log.debug( 'extractToken', pKeyHash )
  return pKeyHash.substring( 0, TKN_LEN )
}

//=============================================================================

async function getPkHash( dbName, collName, doc, pk ) {
  log.debug( 'getPkHash', dbName, collName, doc, pk )
  let keyStr = await genKeyStr( dbName, collName, doc, pk ) 
  // log.info( 'getPkHash', keyStr )
  let pKeyHash = await getKeyHash( keyStr )
  // log.info( 'getPkHash', pKeyHash )
  return pKeyHash
}


async function getKeyHash( keyStr ) {
  const { createHash } = await import('node:crypto')
  const hash = createHash('sha256')
  hash.update( keyStr )
  let keyHash = hash.digest('hex')
  // reduce key size:
  let key = ''
  for ( let i = 0; i < 32; i++ ) {
    let x1 = parseInt( keyHash[ i ], 16 )
    let x2 = parseInt( keyHash[ i + 32 ], 16 )
    key += ( ( x1 + x2 ) % 16 ).toString( 16 )
  }
  log.debug( 'getKeyHash', key, keyHash )
  return key
}


async function genKeyStr( dbName, collName, doc, pk ) {
  log.debug( 'genKeyStr', dbName, collName )
  let keyStr = ''
  //let pk = await persistence.getCollPK( dbName, collName )
  for ( const keyField of pk ) {
    if ( keyStr != '' ) { keyStr += ',' }
    keyStr += keyField + ':' 
    keyStr += doc[ keyField ]
  }
  log.debug( 'genKeyStr >>', pk, keyStr, doc )
  return keyStr
}



function getQrynKeyStr( query, key ) {
  log.debug( 'getQrynKeyStr', query, key )
  let keyStr = ''
  for ( const keyField of key ) {
    if ( keyStr != '' ) { keyStr += ',' }
    keyStr += keyField + ':' 
    keyStr += query[ keyField ]
  }
  log.debug( 'getQrynKeyStr >>', key, keyStr, query )
  return keyStr
}


async function genKeyStr( dbName, collName, doc, pk ) {
  log.debug( 'genKeyStr', dbName, collName )
  let keyStr = ''
  //let pk = await persistence.getCollPK( dbName, collName )
  for ( const keyField of pk ) {
    if ( keyStr != '' ) { keyStr += ',' }
    keyStr += keyField + ':' 
    keyStr += doc[ keyField ]
  }
  log.debug( 'genKeyStr >>', pk, keyStr, doc )
  return keyStr
}


function getTxnId( op ) {
  return op + '.' + randomChar( 10 ) 
}

function randomChar( len ) {
  var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  var token =''
  for ( var i = 0; i < len; i++ ) {
    var iRnd = Math.floor( Math.random() * chrs.length )
    token += chrs.substring( iRnd, iRnd+1 )
  }
  return token
}

function randomHex( len ) { // this is ridiculous complex way :-D
  var chrs = "0123456789abcdef"
  var hexStr =''
  for ( var i = 0; i < len; i++ ) {
    var iRnd = Math.floor( Math.random() * chrs.length )
    hexStr += chrs.substring( iRnd, iRnd+1 )
  }
  return hexStr
}


function getKeyVal( doc, field ) {
  log.info( '>>>>>>>>>>>>>  getKeyVal', doc, field )
  let split = field.indexOf('.')
  if ( split > 0 ) {
    let subField = field.substring( 0, split )
    if ( doc.hasOwnProperty( subField ) ) {
      return { _keyVal: doc[ subField ] }
    } else {
      let nextField = field.substring( split + 1 )
      return getVal( doc[ subField ], nextField )
    }
  } else {
    return { _keyVal: doc[ field ] }
  }
}

function getTokenFromStr( str ) {
  let buf = Buffer.from( str )
  let strHex = buf.toString( 'hex' )
  return strHex.substring( 0, TKN_LEN )
}


//=============================================================================

let dbgTimes = {}
let dbgTxnTS = {}

function dbgStart( method, txnId ) {
  if ( ! MEASURE_PERF ) { return }
  if ( ! txnId ) { txnId = 't'+ Math.random() }
  dbgTxnTS[ txnId ] = Date.now()
  return txnId
}

function dbgStep( end, start, txnId ) {
  if ( ! MEASURE_PERF ) { return }
  dbgEnd( end, txnId )
  dbgStart( start, txnId )
}


function dbgEnd( method, txnId ) {
  if ( ! MEASURE_PERF ) { return }
  let now = Date.now()
  if ( dbgTxnTS[ txnId ] ) {
    if ( ! dbgTimes[ method ] ) { dbgTimes[ method ] = { sum_ms: 0, cnt: 0 } }
    dbgTimes[ method ].sum_ms += now - dbgTxnTS[ txnId ] 
    dbgTimes[ method ].cnt ++
    delete dbgTxnTS[ txnId ]
  }
}

function dbgPrint() {
  if ( ! MEASURE_PERF ) { return }
  log.debug( 'dbgTimes', dbgTimes )
  dbgTimes = {}
  dbgTxnTS = {}
}

setInterval( dbgPrint, 60000 )