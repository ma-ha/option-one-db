const pjson  = require('../package.json')
const log    = require( '../helper/logger' ).log

exports: module.exports = {
  init : init,
  getVer : getVer
}

let upSince = (new Date() ).toISOString()
// ----------------------------------------------------------------------------

function init( app, getHealth ) {
  log.info( 'API init healthz' )

  app.get( '/version', (req, res) => {
    res.send( getVer() )
  })

  app.get( '/healthz', (req, res) => {
    res.send( getHealth() )
  })

}

function getVer() {
  return {
    name    : pjson.name,
    version : pjson.version,
    // env     : config.staging,
    upSince : upSince
  }
}
