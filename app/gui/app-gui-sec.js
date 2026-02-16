const gui     = require( 'easy-web-app' )
const express = require( 'express' )
const log     = require( '../helper/logger' ).log
const db      = require( '../db-engine/db' )
const apiHelper = require( '../api/api-helper' )

//const weblog  = require( './app-weblog.js' ) 

exports: module.exports = {
  init,
  authenticate,
  // changePassword,
  // authorize,
  // createToken,
  // getUserIdForToken,
  // getUserNameForToken,
  // deleteUserIdForToken
}

function init( gui ) {
  
  // switch securiy on:
  gui.enableSecurity({ divLayout: true })
  gui.secParams.sessionExpiredAlert = true 

  // map hooks
  gui.authenticate = authenticate
  gui.changePassword = changePassword
  gui.authorize = authorize
  gui.createToken = createToken
  gui.getUserIdForToken = getUserIdForToken

  //optional
  gui.getUserNameForToken = getUserNameForToken
  gui.deleteUserIdForToken = deleteUserIdForToken
}


// "auth" is ok if any user id is given
async function authenticate( userid, password, callback ) {
  log.info( 'Login user', userid )

  if ( userid == 'admin' && process.env.ADMIN_PWD  && process.env.ADMIN_PWD === password ) {
    db.addAuditLog( 'admin', 'user', 'GUI', 'Login', '-' )
    return callback( null, true, false )
  }

  const [ uid, passwordHash ] = await apiHelper.hashCredentials( userid, password )

  let id = await db.getPkID( 'user:'+userid )
  let findUser = await db.getDocById( 'admin', 'user', id ) // look up in local DB

  let user = findUser.doc
  log.info( 'authenticate user', id )
  log.debug( 'authenticate user', id, passwordHash, user )

  if ( user && user.passwordHash == passwordHash ) {
    db.addAuditLog( userid, 'user', 'GUI', 'Login', '-' )
    callback( null, true, false )
  } else {
    log.info( 'Login failed', userid )
    db.addAuditLog( userid, 'user', 'GUI', 'Login failed', '-' )
    callback( 'Authentication failed', false )
  }
}

// hook for change password requests 
async function changePassword( userId, oldPasswprd, newPassword, callback ) {
  log.warn( 'Change password for:', userId ) //TODO: implement ...
  let id = await db.getPkID( 'user:'+userId )
  let findUser = await db.getDocById( 'admin', 'user', id ) // look up in local DB

  let user = findUser.doc

  const { createHash } = await import('node:crypto')
  const oldHash = createHash('sha256')
  oldHash.update( userId + oldPasswprd )
  let oldPasswordHash = oldHash.digest('hex')

  if ( user && user.passwordHash == oldPasswordHash ) {

    const hash = createHash('sha256')
    hash.update( userId + newPassword )
    let passwordHash = hash.digest('hex')

    let result = await  db.updateOneDoc(
      { 
        db     : 'admin', 
        coll   : 'user', 
        update : { $set: {
          passwordHash      : passwordHash,
          passwordTimestamp : Date.now(),
        }},
        txnId  : 'ChPwd_' + apiHelper.randomChar( 10 ),
        fn     : 'changePassword'
      },
      { _id : id },
      { allNodes: true }
    )

    if ( result.error ) {
      callback( 'Password change failed', false )
    } else {
      callback( null, true )
    }
  } else {
    log.warn( 'Change password old password wrong' )
    callback( 'Change password old password wrong', false )
  }
}


// grant all to "main" page: if user != null then "granted"
async function authorize ( userid, page ) {
  if ( page == 'main') { return true }
  if ( page == 'license-nonav') { return true }
  if ( ! userid ) { return false }

  let id = await db.getPkID( 'user:'+userid )
  let user = await db.getDocById( 'admin', 'user', id ) // look up in local DB
  //log.info( 'user', page, user )
  if ( ! user._ok ) {
    log.info( 'User "'+userid+'" is not authorized for page "'+page+'"' )
    return false
  }
  if ( page == 'userPage' && user.doc.autz.admin != 'w' ) {
    log.info( 'User "'+userid+'" is not authorized for page "'+page+'"' )
    return false  
  }
  // log.info( 'User "'+userid+'" is authorized for page "'+page+'"' )
  return true
}


async function createToken( userId ) {
  log.debug( 'createToken', userId )
  let token = apiHelper.randomChar( 10 )
  let tokenId = await db.getPkID( 'token:'+token )
  let session = { 
    _id      : tokenId, 
    token    : token, 
    user     : userId,
    expireAt : Date.now() + 1000*60*60
    }
  // await db.insertOneDoc( { db: 'admin', coll: 'session' }, session ) 
  await db.insertOneDoc( { db: 'admin', coll: 'session' }, session ) 
  return token 
}

async function getUserIdForToken( token ) {
  log.debug( 'getUserIdForToken', token )
  let sessionId = await db.getPkID( 'token:'+token )
  let session = await db.getDocById( 'admin', 'session', sessionId )
  if ( session._ok ) {
    log.debug( 'getUserIdForToken', session )
    return session.doc.user 
  }
  return null 
}

  //optional
async function getUserNameForToken( token ) {
  let sessionId = await db.getPkID( 'token:'+token )
  let session = await db.getDocById( 'admin', 'session', sessionId )
  if ( session._ok ) {
    return session.doc.user
  }
  return null 
}

function deleteUserIdForToken( token )  {
  // TODO: implement for HA cluster
}