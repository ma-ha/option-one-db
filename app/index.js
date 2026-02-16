/* Copyright Markus Harms 2025 */

const { initLog, log } = require( './helper/logger' )
const mon      = require( './helper/healthCheck' )
const gui      = require( './gui/app-gui' )
const api      = require( './api/app-api' )
const nodeMgr  = require( './cluster-mgr/node-mgr' )
const shutdown = require( './helper/shutdown' )
const db       = require( './db-engine/db' )
const cfgHlp   = require( './helper/config' )

let status = 'OK'
let app = null

// we need crypto
try {
  const crypto = require( 'node:crypto' )
} catch (err) {
  console.error('FATAL: "crypto" support is disabled!')
  process.exit(1)
} 

module.exports = {
  startDB
}

async function startDB( config ) {
  initLog( config )
  log.info( 'Initialize ...')
  // start API and GUI
  app = gui.init( config )
  mon.init( app.getExpress(), () => { return status } )
  api.init( app, config )

  // connect to other cluster nodes
  nodeMgr.init( config )
  db.init( config, nodeMgr )
  
  // let allCFG = cfgHlp.getAllConfigs()
  // for ( let cfg in allCFG ) {
  //   console.log( '| '+ ( cfg +'                              ').substring(0,25) + ' | '+ allCFG[ cfg ] + '|' )
  // }

  // register callbacks for a clean shutdown
  shutdown.registerCallback( api.terminate )
  shutdown.registerCallback( nodeMgr.terminate )

  process.on( 'SIGTERM', shutdown.doShutdown )
  process.on( 'SIGINT',  shutdown.doShutdown )
  process.on( 'SIGQUIT', shutdown.doShutdown )

  stopInterval = setInterval( checkStopReq, 10000 )
}

let stopInterval = null 

async function checkStopReq() {
  if ( nodeMgr.checkStopReq() ) {
    await shutdown.doStop()
    clearInterval( stopInterval )
  }
}