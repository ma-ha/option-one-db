const log     = require( '../helper/logger' ).log
const nodeMgr = require( '../cluster-mgr/node-mgr' )

module.exports = {
  addNode
}

// ============================================================================
// new node onboarding
async function addNode( req, res ) {
  log.info( 'GET /cluster/add', req.query.podName ) // link from GUI
  try {
    await nodeMgr.onboardNode( req.query.podName )
  } catch ( exc ) {
    log.error( 'addNode', exc )
  }
  res.redirect( '../index.html?layout=main' )
}