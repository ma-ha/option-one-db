const log  = require( './logger' ).log

const SHUTDOWN_TIMEOUT_MS = 15000


exports: module.exports = {
  registerCallback,
  doShutdown,
  doStop
}

const shutdownCallbacks = []

function registerCallback( shutdownFunction  ) {
  shutdownCallbacks.push( shutdownFunction )
}

async function doShutdown() {
  log.warn('Shutdown ...')

  // Watch dog the shutdown
  setTimeout( () => {
    log.fatal( 'SHUTDOWN FAILED' )
    process.exit( 1 )
  }, SHUTDOWN_TIMEOUT_MS )


  let terminationPromises = []
  for ( let callbackName of shutdownCallbacks ) {
    terminationPromises.push( eval( callbackName )() )
  }

  await Promise.allSettled( terminationPromises )

  log.info( 'Shutdown OK, exiting' )
  process.exit( 0 )
}

async function doStop() {
  log.warn( 'Stop DB ...')
  let terminationPromises = []
  for ( let callbackName of shutdownCallbacks ) {
    terminationPromises.push( eval( callbackName )() )
  }
  await Promise.allSettled( terminationPromises )
  log.info( 'Write operations stopped!' )
}
