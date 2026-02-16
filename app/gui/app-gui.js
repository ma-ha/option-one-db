const gui       = require( 'easy-web-app' )
const express   = require( 'express' )
const log       = require( '../helper/logger' ).log
const cfgHlp    = require( '../helper/config' )
const pjson     = require( '../package.json' )
const sec       = require( './app-gui-sec' )
//const weblog  = require( './app-weblog.js' ) 

const mainPg     = require( './gui-page-main' )
const clusterGUI = require( './gui-page-cluster' )
const dbGUI      = require( './gui-page-db' )
const userPg     = require( './gui-page-user' )
const apiSpPg    = require( './gui-page-api-sp' )
const monitoring = require( './gui-page-monitoring' )
const backupPg   = require( './gui-page-backup' )

exports: module.exports = {
  init
}

let cfg = {
  APP_NAME: 'Option-One DB',
  API_PATH: '/db',
  PORT: 9000,
  GUI_SHOW_CLUSTER : true,
  GUI_SHOW_ADD_DB  : true,
  GUI_SHOW_USER_MGMT : true,
  DB_PASSWORD_REGEX_HINT: "Password minimum length must be 8, must contain upper and lower case letters, numbers and extra characters !@#$&*+-_=[]{}",
  ERR_LOG_EXPIRE_DAYS : 31
}

function init( configParams ) {
  log.info( 'Starting gui' )
  cfgHlp.setConfig( cfg, configParams )

  gui.init( cfg.APP_NAME, cfg.PORT, cfg.API_PATH )
  // gui.getExpress().use( '/css-custom', express.static( __dirname + '/css' ) )
  gui.getExpress().use( '/ext-module', express.static( __dirname + '/ext-module' ) )
  gui.getExpress().use( '/img', express.static( __dirname + '/img' ) )
  //gui.express.use( weblog() )
  
  mainPg.init( gui, cfg.APP_NAME )

  if ( cfg.GUI_SHOW_CLUSTER ) {
    clusterGUI.addClusterPage( gui )
  }

  dbGUI.addDatabasePage( gui, cfg.GUI_SHOW_ADD_DB )
  backupPg.addBackupPage( gui )

  // dbGUI.addScriptPage( gui )

  if ( cfg.GUI_SHOW_USER_MGMT ) {
    userPg.addUserPage( gui, cfg.DB_PASSWORD_REGEX_HINT )
  }

  apiSpPg.addApiSpPage( gui )
  userPg.addAuditLogPage( gui )
  monitoring.addMonitoringPage( gui, cfg.ERR_LOG_EXPIRE_DAYS )
  
  addHelpPage( gui )
  addLicensePage( gui )

  gui.pages['main']['header'].linkList = [
    { text: "Toggle design", url: "gui/toggle-design"}
  ]
  gui.pages['main'].footer.copyrightText =  cfg.APP_NAME + ' v'+pjson.version+ ' &#169; Markus E. Harms 2026'
  gui.pages['main'].addFooterLink( 'License', 'index.html?layout=license-nonav' )
  gui.pages['main'].addFooterLink( 'Support',  'https://option-one-db.online-service.cloud'  )
  gui.pages['main'].addFooterLink( 'GitHub',  'https://github.com/ma-ha/'  )

  sec.init( gui )

  gui.getExpress().get( '/gui/static/welcome/html', (req,res) => {
    res.send('<img src="img/welcome.png" class="centerimg">')
  }) 

  gui.getExpress().get( '/gui/none/html', (req,res) => {
    res.send( 'bla' )
  }) 

  gui.getExpress().get( '/gui/db/names', (req,res) => {
    [ { "name":"1"}, { "name":"XYZ" } ]
  })

  return gui
}

// ============================================================================

function addHelpPage( gui ) {
  let helpPg = gui.addPage( 'helpPage', 'Help', {
    id    : 'Wiki',
    title : 'Docu',
    type  : 'pong-markdown',
    resourceURL : 'gui/wiki/',
    height: '790px'
  },
  {
    page  : '${lang}/${page}',
    start : 'main.md',
    edit  : false
  })
  helpPg.navLabel = 'Help'
  helpPg.title    = 'Help'
  helpPg.setPageWidth( '90%' )
}


function addLicensePage( gui ) {
  const pg = gui.addPage( 'license-nonav', 'License')
  pg.setPageWidth( '80%' )

  var columns = pg.addColumnsRow( 'row3', '790px' )
  var col1 = columns.addRowsColumn( 'col1', '70%' )
  var col2 = columns.addRowsColumn( 'col2', '20%' )

  col1.addView({
    id    : 'License',
    title : 'License',
    type  : 'pong-markdown',
    resourceURL : 'gui/license/',
    height: '790px'
  },
  {
    page  : '${lang}/${page}',
    start : 'license.md',
    edit  : false
  })

  col2.addView({
    id    : 'LicenseHint',
    title : 'Remark',
    type  : 'pong-markdown',
    resourceURL : 'gui/license/',
    height: '790px'
  },
  {
    page  : '${lang}/${page}',
    start : 'license-note.md',
    edit  : false
  })
}