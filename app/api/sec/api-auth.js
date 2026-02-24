const log     = require( '../../helper/logger' ).log
const cfgHlp  = require( '../../helper/config' )
const db      = require( '../../db-engine/db' )
const apiHelper = require( '../api-helper' )
const helper  = require( '../../db-engine/db-helper' )

exports: module.exports = { 
  initSecCheck,
  // initReplicaCheck,
  initAdminCheck,
  userAutzForSvc
}

// ----------------------------------------------------------------------------

let cfg = {
  DB_CLUSTER_NAME: 'db1',
  DB_CLUSTER_KEY : 'db1-key',
  DB_DISABLE_ADMIN_AUTZ: 'no',
  DB_DISABLE_USER_AUTZ : 'no'
}

function initAdminCheck( configParams ) {
  cfgHlp.setConfig( cfg, configParams )

  let check = async (req, res, next) => {
    try {
      log.debug( 'AdminJWTcheck', req.url,  req.method, req.headers )
      log.debug( 'AdminJWTcheck', 'API call is authorized' )

      // this is only for development
      if ( cfg.DB_DISABLE_ADMIN_AUTZ === 'yes' ) {
        log.warn( 'Admin authorization disabled' )
        return await next()
      }

      if ( req.headers[ 'accessid'] ) {
        // log.info( 'HDR', req.headers )
        let dbName = req.header.accessid.split('/')[0]
        if ( dbName == 'all_') {
          dbName = '*'
        } else {
          log.warn( 'Unauthorizated',  )
          return next( new UnauthorizedError(
            'accessId/accessKey check failed', 
            { message: 'accessId/accessKey check failed' }
          ))
        }
        let spId = req.headers.accessid.split('/')[1]
        let key = req.headers.accessid
        let apiAccess = await db.getDocById( 'admin', 'api-access', spId )
        log.debug( 'apiAccess', apiAccess )
        if ( apiAccess?.doc ) {
          let keyHash = await db.getPkID( key )
          log.debug( 'Admin ID', dbName, spId, keyHash, apiAccess?.doc?.keyHash )
          if ( apiAccess?.doc?.keyHash === keyHash ) {
            req.xUserAuthz = {}
            req.xUserAuthz[ dbName ] = 'w'
            req.xUser = req.headers.accessid
            return await next()
          }
        }
        // ...else
        return next( new UnauthorizedError(
          'accessId/accessKey check failed', 
          { message: 'accessId/accessKey check failed' }
        ))
      }

      let user = await getBasicAuthUser( req )
      if ( ! user ) {
        let uid = await gui.getUserIdFromReq( req )
        log.debug( 'initAdminCheck from req', uid )
        if ( uid ) {
          let dbId = await db.getPkID( 'user:'+uid )
          let dbUser = await db.getDocById( 'admin', 'user', dbId )
          if ( dbUser?.doc ) {
            user = dbUser.doc
          }
        }
      }
      log.debug( 'initAdminCheck', user )
      if ( ! user ) {
        return next( new UnauthorizedError(
          'Authorization failed', 
          { message: 'Authorization failed' }
        ))  
      }
      if ( ! user.autz.admin === 'w' ) {
        return next( new UnauthorizedError(
          'Admin authorization failed', 
          { message: 'Admin authorization failed' }
        ))
      }
    
      req.xUser = user._id
      req.xUserAuthz = user.autz


      return await next() 
    } catch ( exc ) {
      log.error( 'AdminJWTcheck', exc )
      return next( new UnauthorizedError(
        'DB Authorization failed', 
        { message: 'DB Authorization failed' }
      ))  
    }
  }
  return check
}

// ----------------------------------------------------------------------------
// Authorization Checker
let gui = null
function initSecCheck( guiApp) {
  gui = guiApp

  let check = async (req, res, next) => {
    try {
      let txnId = helper.dbgStart( 'SecCheck' )

      log.debug( 'SecCheck', req.url, req.headers )

      if ( req.url.startsWith( '/gui/license/EN/' ) ) {
        return await next()
      }
      
      if ( req.headers[ 'x-cluster'] ) {
        log.warn( 'ReplicaCheck x-cluster', addr + path )
        log.debug( 'ReplicaCheck', req.headers )
        if ( req.headers[ 'x-cluster'] !== cfg.DB_CLUSTER_NAME  || 
            req.headers[ 'x-key']     !== cfg.DB_CLUSTER_KEY  ) {
          log.fatal( 'ReplicaCheck are DB_CLUSTER_NAME/DB_CLUSTER_KEY set?' )
          helper.dbgEnd( 'SecCheck', txnId )
          return next( new UnauthorizedError(
            'DB_CLUSTER_NAME/DB_CLUSTER_KEY check failed', 
            { message: 'DB_CLUSTER_NAME/DB_CLUSTER_KEY check failed' }
          ))
        } else {
          helper.dbgEnd( 'SecCheck', txnId )
          return await next()
        }
      }

      if ( req.headers[ 'accessid'] ) {
        log.debug( 'HDR', req.headers )
        let dbName = req.headers.accessid.split('/')[0]
        if ( dbName == 'all_') {
          dbName = '*' 
        }
        let spId = req.headers.accessid.split('/')[1]
        let key = req.headers.accesskey
        let apiAccess = await db.getDocById( 'admin', 'api-access', spId )
        log.debug( 'apiAccess', apiAccess )
        if ( apiAccess?.doc ) {
          let keyHash = await db.getPkID( key )
          log.debug( 'ID', dbName, spId, keyHash, apiAccess?.doc?.keyHash )
          if ( apiAccess?.doc?.keyHash === keyHash ) {
            req.xUserAuthz = {}
            req.xUserAuthz[ dbName ] = 'w'
            req.xUser = req.headers.accessid
            helper.dbgEnd( 'SecCheck', txnId )
            return await next()
          }
        }
        // ...else
        helper.dbgEnd( 'SecCheck', txnId )
        return next( new UnauthorizedError(
          'accessId/accessKey check failed', 
          { message: 'accessId/accessKey check failed' }
        ))
      }

      log.debug( 'SecCheck', req.params )
      if ( req.params.db === 'admin' ) {
        helper.dbgEnd( 'SecCheck', txnId )
        return next( new UnauthorizedError(
          'Call mot allowed for "admin" db', 
          { message: 'Call mot allowed for "admin" db' }
        ))      
      } 

      // this is only for development
      if ( cfg.DB_DISABLE_USER_AUTZ === 'yes' ) {
        log.warn( 'User authorization disabled' )
        helper.dbgEnd( 'SecCheck', txnId )
        return await next()
      }

      // check if this request comes from the internal web GUI
      let xUser = await gui.getUserIdFromReq( req )
      log.debug( 'SecCheck xUser', xUser )
      if ( xUser ) {
        let id = await db.getPkID( 'user:'+xUser )
        let user = await db.getDocById( 'admin', 'user', id )
        log.debug( 'SecCheck user', user )
        if ( ! user || ! user.doc ) {
          helper.dbgEnd( 'SecCheck', txnId )
          return next( new UnauthorizedError(
            'Authorization failed', 
            { message: 'Authorization failed' }
          ))  
        }
        req.xUserAuthz = user.doc.autz
        req.xUser = xUser
        req.xCSS  = ( user.doc.design ?  user.doc.design : 'bright' )
        helper.dbgEnd( 'SecCheck', txnId )
        return await next()
      }
      log.debug( 'SecCheck headers.authorization' )

      if ( ! req.headers.authorization ) {
        helper.dbgEnd( 'SecCheck', txnId )
        if ( req.url.startsWith( '/css-custom' ) ) {
          return await next()
        }
        if ( req.url == '/gui/toggle-design' ) {
          return await next()
        }
  
        return next( new UnauthorizedError(
          'Authorization required', 
          { message: 'Authorization required' }
        ))      

      }

      let user = await getBasicAuthUser( req )
      log.debug( 'SecCheck getBasicAuthUser', user )
      if ( ! user ) {
        helper.dbgEnd( 'SecCheck', txnId )
        if ( req.url.startsWith( '/css-custom' ) ) {
          return await next()
        }
        if ( req.url == '/gui/toggle-design' ) {
          return await next()
        }  
        return next( new UnauthorizedError(
          'Authorization failed', 
          { message: 'Authorization failed' }
        ))  
      }
      if ( req.params.db && ! user.autz[ req.params.db ]  && ! user.autz[ '*' ] ) { // TODO check 'w' and 'r'
        helper.dbgEnd( 'SecCheck', txnId )
        return next( new UnauthorizedError(
          'DB Authorization failed', 
          { message: 'DB Authorization failed' }
        ))  
      }
      log.debug( 'JWTcheck', 'API call is authorized' )
      req.xUser = user.user
      req.xUserAuthz = user.autz
      helper.dbgEnd( 'SecCheck', txnId )
      return await next()
    } catch ( exc ) {
      log.error( 'SecCheck', exc )
      return next( new UnauthorizedError(
        'DB Authorization failed', 
        { message: 'DB Authorization failed' }
      ))  
    }
  }
  return check
}

// ============================================================================
async function getBasicAuthUser( req ) {
   // parse headers
   const b64auth = ( req.headers.authorization || '' ).split( ' ' )[1] || ''
   const [ userId, password ] = Buffer.from( b64auth, 'base64' ).toString().split( ':' )

  const [ uid, passwordHash ] = await apiHelper.hashCredentials( userId, password )

  let id = await db.getPkID( 'user:'+userId )

   // verify 
   let user = await db.getDocById( 'admin', 'user', id )
   log.debug( 'getBasicAuthUser', user )
  //  log.info( 'getBasicAuthUser', passwordHash )

   //  log.info( 'user', user )
   if ( user._ok && user.doc.passwordHash == passwordHash ) {
     return user.doc
   }
   return null
}


// ============================================================================

function UnauthorizedError (code, error) {
  this.name    = "UnauthorizedError"
  this.message = error.message
  Error.call( this, error.message )
  Error.captureStackTrace( this, this.constructor )
  this.code   = code
  this.status = 401
  this.inner  = error
}

UnauthorizedError.prototype = Object.create(Error.prototype);
UnauthorizedError.prototype.constructor = UnauthorizedError;

// ============================================================================

let autzCache = {}

async function userAutzForSvc( req, res  ) {
  log.debug( 'userAutzForSvc' )
  return {} // TODO implement real autorization
}
