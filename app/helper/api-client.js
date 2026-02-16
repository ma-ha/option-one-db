const log      = require( '../helper/logger' ).log
const axios    = require( 'axios' )

module.exports = {
  apiErr,
  post,
  postDbg,
  get
}

// ============================================================================

let errCnt = 0

function apiErr() {
  return errCnt
}


async function post( addr, path, dta, debug ) { // TODO simplify to async axios api
  return new Promise( ( resolve, reject ) => {
    log.debug( 'POST', addr + path )
    if ( debug ) log.info( 'POST', addr + path )
    try {
      log.warn( 'POST x-cluster', addr + path )
      axios.post( 
        'http://'+ addr + path, 
        dta,
        { headers: {
            'x-cluster' : process.env[ 'DB_CLUSTER_NAME' ],
            'x-key'     : process.env[ 'DB_CLUSTER_KEY' ],
          } 
        }
      ).then( req => {
        log.debug( 'POST done', req.data )
        if ( debug ) log.info( 'POST done', req.data )
        if ( req.request.res.statusCode != 200 ) {
          log.warn( (new Date()).toISOString(), 'send', req.request.res.statusMessage )
          //log.warn( (new Date()).toISOString(), 'send', req.request.res )
          errCnt ++
          return resolve({ error: req.request.res.statusMessage })
        }
        if ( errCnt > 0 ) { errCnt-- }
        resolve( req.data )
      }).catch( error => {
        log.warn(  (new Date()).toISOString(), 'POST .catch', addr+path, error.message )
        errCnt ++
        resolve({ error:error.message })
      })
    } catch ( exc ) {
      log.fatal( (new Date()).toISOString(), 'POST catch', addr+path, exc.message )
      errCnt ++
      resolve({ error: exc.message })
    }
  })
}

async function postDbg( addr, path, dta ) {
  return new Promise( ( resolve, reject ) => {
    log.info( 'POST', addr + path )
    try {
      axios.post( 
        'http://'+ addr + path, 
        dta,
        { headers: {
          'x-cluster' : process.env[ 'DB_CLUSTER_NAME' ],
          'x-key'     : process.env[ 'DB_CLUSTER_KEY' ],
          } 
        }
      ).then( req => {
        log.info( 'POST done', req.data )
        if ( req.request.res.statusCode != 200 ) {
          log.warn( (new Date()).toISOString(), 'send', req.request.res.statusMessage )
          //log.warn( (new Date()).toISOString(), 'send', req.request.res )
          errCnt ++
          return resolve({ error: req.request.res.statusMessage })
        }
        if ( errCnt > 0 ) { errCnt-- }
        resolve( req.data )
      }).catch( error => {
        log.warn(  (new Date()).toISOString(), 'POST .catch', addr+path, error.message )
        errCnt ++
        resolve({ error:error.message })
      })
    } catch ( exc ) {
      log.fatal( (new Date()).toISOString(), 'POST catch', addr+path, exc.message )
      errCnt ++
      resolve({ error: exc.message })
    }
  })
}


async function get( addr, path, dta ) {
  return new Promise( ( resolve, reject ) => {
    log.debug( 'GET', addr + path )
    try {
      axios.get( 'http://'+ addr + path, 
        { params: dta,
          headers: {
            'x-cluster' : process.env[ 'DB_CLUSTER_NAME' ],
            'x-key'     : process.env[ 'DB_CLUSTER_KEY' ],
          } 
        }
      ).then( req => {
        if ( req.request.res.statusCode != 200 ) {
          log.warn( (new Date()).toISOString(), 'send', req.request.res.statusMessage )
          //log.warn( (new Date()).toISOString(), 'send', req.request.res )
          errCnt ++
          return resolve({ error: req.request.res.statusMessage })
        }

        if ( errCnt > 0 ) { errCnt-- }
        //log.info( 'req.data', req.data)
        resolve( req.data )
      }).catch( error => {
        log.warn( (new Date()).toISOString(), 'GET .catch',addr + path, error.message )
        errCnt ++
        resolve({ error:error.message })
      })
    } catch ( exc ) {
      log.fatal( (new Date()).toISOString(), 'GET catch',addr + path, exc.message )
      errCnt ++
      resolve({ error: exc.message })
    }
  })
}
