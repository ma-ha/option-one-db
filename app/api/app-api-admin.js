const cfgHlp  = require( '../helper/config' )
const { log, getMonitoringCfg } = require( '../helper/logger' )
const db      = require( '../db-engine/db' )
const { httpSatusCodes : st }  = require( './http-codes' )
const apiHelper = require( './api-helper' )

module.exports = {
  init,
  getUser,
  addUser,
  changeUserPassword,
  addUserAutz,
  rmUserAutz,
  changeUserAutz,
  delUser,

  getSP,
  addSP,
  delSP,

  getMonitoring,
  // saveMonitoring,
  getLogs
}

// ============================================================================

let cfg = {
  DB_PASSWORD_REGEX: '^(?=.*[A-Z].*)(?=.*[!@#$&*+\]\}\[\{\-_=].*)(?=.*[0-9].*)(?=.*[a-z].).{8,}$',
  DB_PASSWORD_REGEX_HINT: "Password minimum length must be 8, must contain upper and lower case letters, numbers and extra characters !@#$&*+-_=[]{}",
  API_KEY_LENGTH: 20
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================

async function getUser( req, res ) {
  let txnId = db.getTxnId( 'USR' )
  try {
    log.info( txnId, 'API getUser...' ) 
    let result = []
    let dbArr   = await db.listDBs(  )
    let userArr = await db.listUserRights()
    // log.info ( 'userarr ', userArr)
    // log.info ( 'dbarr ', dbArr )

    for ( let user of userArr ) {
      let userRow = { _userid: user.userid, _email: user.email, _all: false, _admin: false }
      if ( user.autz[ 'admin' ] === 'w' ) { userRow[ '_admin' ] = true }  
      if ( user.autz[ '*' ] === 'w' ) { userRow[ '_all' ] = true }  
      for ( let dbName of dbArr ) {
        if ( user.autz[ dbName ] === 'w' ) {
          userRow[ dbName ] = true 
        } else {
          userRow[ dbName ] = false
        }  
      }
      result.push( userRow )
    }
    // log.info( 'user', result)
    res.send( result )
  } catch ( exc ) {
    log.error( txnId, 'addUser', exc )
    res.send( 'Get user failed: ' + exc.message )
  }
}

// ============================================================================

async function addUser( req, res ) {
  let txnId = db.getTxnId( 'INS' )
  log.info( txnId, 'API addUser...' ) //, req.body) 
  if ( typeof req.body.user === 'string' && //req.body.user.length >= 2 && 
       typeof req.body.password  === 'string' ) {
    try {
           
      const passwordChk = new RegExp( cfg.DB_PASSWORD_REGEX )
      // log.info( txnId, '----', passwordChk.test( req.body.password ), req.body.password )
      if ( ! passwordChk.test( req.body.password )  ) {
        return sndBadRequest( res, 'addUser', 'Lousy Passsword\n'+cfg.DB_PASSWORD_REGEX_HINT, txnId ) 
      }  
    
      const [ uid, passwordHash ] = await apiHelper.hashCredentials( req.body.user, req.body.password  )
      
      let id = await db.getPkID( 'user:'+req.body.user )
      
      // TODO: Check if user already exists

      let user = {
        _id               : id,
        user              : req.body.user,
        passwordHash      : passwordHash,
        passwordTimestamp : Date.now(),
        autz : {}
      }
      if ( req.body.db ) { // todo check if string
        user.autz[ req.body.db ] = 'w'
      }
      if ( req.body.email ) { // todo check if string
        user.email = req.body.email 
      }
      if ( req.body.admin === true ) {
        user.autz[ 'admin' ] = 'w'
      }
      let insertResult = await db.insertOneDoc( { db: 'admin' , coll: 'user', txnId: txnId }, user)
      if ( insertResult._ok ) {
        db.addAuditLog( req.xUser, 'user', req.body.user, 'Add user', txnId )
        res.send( 'Add user: ' + insertResult._ok )
      } else {
        db.addAuditLog( req.xUser, 'user', req.body.user, 'Add user failed', txnId )
        res.send( 'Add user failed: ' + insertResult._error )
      }
    } catch ( exc ) {
      log.error( txnId, 'addUser', exc )
      res.send( 'Add user failed: ' + exc.message )
    }

  } else {
      sndBadRequest( res, 'addUser', 'user and password required', txnId )
  }
}

async function changeUserPassword( req, res ) {
  log.warn( 'API changeUserPassword...' ) 
  res.send( 'TODO' )
}

async function addUserAutz( req, res ) {
  log.warn( 'API addUserAutz...' ) 
  res.send( 'TODO' )
}

async function rmUserAutz( req, res ) {
  log.warn( 'API rmUserAutz...' ) 
  res.send( 'TODO' )
}

// ============================================================================

async function changeUserAutz( req, res ) {
  let txnId = db.getTxnId( 'UPD' )
  try {
    log.info( txnId, 'API changeUserAutz...', req.body ) 
    if ( ! req.body._userid ) { return res.status(400).send( '_userid required' ) }
    // let buf = Buffer.from( req.body._userid  )
    let id = await db.getPkID( 'user:'+req.body._userid)

    let user = await db.getDocById( 'admin', 'user', id )
    log.info( txnId, 'API changeUserAutz...', id, user )
    if ( user._ok ) {
      for ( let param in req.body ) {
        if ( param == '_userid'   ) { continue }
        if ( param == '_password' ) { continue }
        let writePerm = (  req.body[ param ] == 'true' ? 'w' : '-' )
        if ( param == '_admin' ) {
          user.doc.autz[ 'admin' ] = writePerm
        } else if ( param == '_all' ) {
          user.doc.autz[ '*' ] = writePerm
        } else {
          user.doc.autz[ param ] = writePerm
        } 
      }
      
    }
    log.info( txnId, 'API user', user )
    let result = await  db.updateOneDoc(
      { 
        db   : 'admin', 
        coll : 'user', 
        update  : { $set: user.doc },
        txnId : txnId
      },
      {
        _id   : id,
        user  : req.body._userid 
      },
      { allNodes: true }
    )

    db.addAuditLog( req.xUser, 'user', req.body._userid , 'Change user ' + JSON.stringify( user.doc.autz ), txnId )

    if ( result._ok ) {
      res.send( result )
    } else {
      res.status( st.SERVER_ERROR ).send( result._error )
    }
  } catch ( exc ) {
    log.error( txnId, 'changeUserAutz', exc )
    res.status( st.SERVER_ERROR ).send( result._error )
  }
}
// ============================================================================

async function delUser( req, res ) {
  let txnId = db.getTxnId( 'DEL' )
  try {
    log.warn( txnId, 'API MIGR delUser...', req.query ) 
    if ( ! req.query._userid ) { return res.status(400).send( '_userid required' ) }

    let id = await db.getPkID( 'user:'+req.query._userid)
    let result = await db.deleteOneDocAllNodes( {db: 'admin', coll : 'user', txnId : txnId }, id )
    db.addAuditLog( req.xUser, 'user', req.query._userid, 'Delete user', txnId)

    if ( result._ok ) {
      res.send( result )
    } else {
      res.status( st.SERVER_ERROR ).send( result._error )
    }
  } catch ( exc ) {
    log.error( txnId, 'delUser', exc )
    res.status( st.SERVER_ERROR ).send( result._error )
  }

}

// ============================================================================
// SP
// {
//   _id     : SP_ID,
//   db      : "mocha-test-db",
//   appName : "load=test",
//   keyHash : "9d42e7b317c627f71d985a1cf2ebfd40",
//   access  : "Database API",
//   expires : 1772172558205
// }

async function getSP( req, res ) {
  let txnId = db.getTxnId( 'SP' )
  try {
    log.debug( 'API getSP...',   req.xUserAuthz ) 
    let result = []
    let allSPs = await db.find( 'admin', 'api-access' )
    // log.info( 'SP', allSPs )
    if ( allSPs._ok ) {
      for ( let sp of allSPs.data ) {
        if ( req.xUserAuthz['*'] || req.xUserAuthz[ sp.db ] ) {
          let expires = '-'
          let delApp = 'Deactivate'
          if ( sp.expires ) {
            let expiresDate = new Date( sp.expires ) 
            expires = expiresDate.toISOString().substring ( 0, 16 ).replace('T',' ')
            log.debug( 'expiresDate', expiresDate.getTime(), Date.now())
            if ( expiresDate.getTime() < Date.now() ) {
              expires = '<span style="color: red;">' + expires + '</span>'
              delApp = false
            }
          }
          result.push({ 
            id      : sp._id, 
            db      : sp.db, 
            accId   : sp.db +'/'+ sp._id,
            app     : sp.appName, 
            expires : expires, 
            access  : sp.access,
            delApp  : delApp
          })
        }
      }
    }
    return res.send( result )
  } catch ( exc ) {
    log.error( txnId, 'getSP', exc )
    res.status( st.SERVER_ERROR ).send( result._error )
  }
}


async function addSP( req, res ) {
  let txnId = db.getTxnId( 'SP' )
  try {
    log.info( 'API addSP...',  req.body ) 
    if ( ! req.body.db ) { return res.send('DB param required') }
    let dbDetails = await db.getDB( req.body.db ) 
    if ( ! dbDetails ) { return res.send('DB not found') }
    if ( ! req.body.app ) { return res.send('App name required') }
    if ( ! req.body.access || ! ( req.body.access == "Database API" || req.body.access == "Admin API" ) ) { 
      return res.send('Access type must be "Database API" or "Admin API"') 
    }

    let spId = apiHelper.randomHex( 15 )
    let key  = apiHelper.randomPw( cfg.API_KEY_LENGTH )
    let expires = null
    if ( req.body.expires == '3m' ) {
      expires = Date.now() + 3*30*24*60*60*1000
    } else   if ( req.body.expires == '1y' ) {
      expires = Date.now() + 365*24*60*60*1000
    }

    let newSP = {
      _id     : spId,
      db      : req.body.db,
      appName : req.body.app,
      keyHash : await db.getPkID( key ),
      access  : req.body.access,
      expires : expires
    }
    // log.info( txnId, 'KEY', key, await db.getPkID( key ) )
    let result = await db.insertOneDoc( { db: 'admin', coll: 'api-access' }, newSP )
    if ( result._error ) {
      return res.send( 'Failed ' + result._error )
    }
    db.addAuditLog( req.xUser, 'sp',  req.body.db+'/'+spId, 'Add api access', 'SP.'+spId )

    return res.send( 'AccessId: '+req.body.db+'/'+spId+'     AccessKey: '+key +'    Please copy this key, it is only shown one time!' )
  } catch ( exc ) {
    log.error( txnId, 'addSP', exc )
    res.status( st.SERVER_ERROR ).send( result._error )
  }
}


async function delSP( req, res ) {
  let txnId = db.getTxnId( 'SP' )
  try {
    log.info( txnId, 'API delSP...',  req.query ) 
    if ( ! req.query.id ) { return res.send('SP ID required') }
    let dbAccessResult = await db.getDocById( 'admin', 'api-access', req.query.id )
    if ( dbAccessResult?.doc ) {
      let sp = dbAccessResult.doc
      sp.expires = Date.now()
      let txnId = db.getTxnId( 'SP' )
      await db.replaceOneDoc( txnId, 'admin', 'api-access', req.query.id, sp, { allNodes: true } )
      db.addAuditLog( req.xUser, 'sp', req.query.db+'/'+req.query.id, 'Revoke api access', txnId )
      return  res.send( 'Access deactivated!' )
    }
    res.send( 'Failed' )
  } catch ( exc ) {
    log.error( txnId, 'delSP', exc )
    res.status( st.SERVER_ERROR ).send( result._error )
  }
}

// ============================================================================

async function getMonitoring( req, res ) {
  let monitoring = getMonitoringCfg()
  res.send( monitoring )
}


// async function saveMonitoring( req, res ) {
//   let monitoring = {
//     _id           : '0000000000000',
//     telemetryURL  : req.body.telemetryURL,
//     enablePings   : ( req.body.enablePings ? true : false ),
//     enableError   : ( req.body.enableError ? true : false ),
//     enableMetrics : ( req.body.enableMetrics ? true : false )
//   }
//   // log.info( txnId, 'KEY', key, await db.getPkID( key ) )
//   let result = await db.insertOneDoc( { db: 'admin', coll: 'monitoring' }, monitoring )
//   if ( result._error ) {
//     return res.send( 'Failed ' + result._error )
//   }
//   db.addAuditLog( req.xUser, 'admin', 'monitoring', 'Monitoring config changed', 'mon' )
//   res.send( 'OK' )
// }


async function getLogs( req, res ) {
  let result = []
  try {
    let logs = await db.find( 'admin', 'log' )
    // log.info( 'getLogs', logs.data.length )
    if ( logs._ok ) {
      let filter = req.query.dataFilter
      if ( filter?.log != '' || filter?.sv != ''  || filter?.dt != '' ) {
        let dt = null
        let dtEnd = 0
        if ( filter.dt ) {
          dt = ( new Date( filter.dt) ).getTime()
          dtEnd = dt + 24*60*60*1000
          log.info( 'getLogs', dt, dtEnd )
        }
        // log.info( 'getLogs',filter )

        for ( let l of logs.data ) {
          if ( dt && ( l.t < dt || l.t > dtEnd ) ) { continue }
          if ( filter.log && filter.log != '' ) { 
            if ( l.m.indexOf( filter.log ) == -1 ) { continue }
          }
          if ( filter.sv && filter.sv != '*' ) {
            if ( filter.sv == 'ERROR' && l.l == "WARN" ) { continue }
            if ( filter.sv == 'FATAL' && l.l != "FATAL" ) { continue }
          }
          l.dt = ( new Date( l.t ) ).toISOString().substring( 0, 19).replace('T',' ')
          result.push( l )
        }
      } else {
        for ( let l of logs.data ) {
          l.dt = ( new Date( l.t ) ).toISOString().substring( 0, 19 ).replace('T',' ')
          result.push( l )
        }
      }
      
      result.sort( (a,b) => { 
        return ( a.t > b.t ? -1 : 1 ) } 
      )
      return res.send( result )
    }
  } catch ( exc ) {
    log.warn( 'getLogs', exc )
  }
  res.send( result )
}

// ============================================================================
// helper

function sndBadRequest( res, fnName, errTxt, txnId ) {
  log.warn( txnId, fnName, 'Bad request', errTxt )
  res.status( st.BAD_REQUEST ).send( errTxt ) 
  return false
}
