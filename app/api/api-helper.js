
module.exports = {
  hashCredentials,
  randomChar,
  randomPw,
  randomHex
}

// ============================================================================

async function hashCredentials( user, password ) {

  const { createHash } = await import('node:crypto')
  const hash = createHash('sha256')
  hash.update( user + password )
  let passwordHash = hash.digest('hex')

  let buf = Buffer.from( user )
  const uid = buf.toString('hex')

  return [ uid, passwordHash ]
}

// ============================================================================


function randomChar( len ) {
  var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  var token =''
  for ( var i = 0; i < len; i++ ) {
    var iRnd = Math.floor( Math.random() * chrs.length )
    token += chrs.substring( iRnd, iRnd+1 )
  }
  return token
}

function randomPw( len ) {
  var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!£$%=-+#<>"
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