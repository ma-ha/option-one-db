const log     = require( '../helper/logger' ).log
const cfgHlp    = require( '../helper/config' )

const swaggerUi = require( 'swagger-ui-express' )
const swagger   = require( './swagger.json' )

module.exports = {
  init
}

let cfg = {
  API_URL: 'http://localhost:9000/db'
}

function init( gui, configParams ) {
  log.info( 'Start swagger GUI...', configParams )
  cfgHlp.setConfig( cfg, configParams )

  const app = gui.getExpress()

  swagger.servers[0].url = cfg.API_URL
  app.use( '/api-docs', swaggerUi.serve, swaggerUi.setup( swagger ) )
}
