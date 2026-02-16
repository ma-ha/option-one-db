const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log
const parser  = require( 'body-parser' )
const upload  = require( 'express-fileupload' )
const sec     = require( './sec/api-auth' )
const cluster = require( './app-api-cluster' )
const db      = require( './app-api-db' )
const data    = require( './app-api-data' )
const gui     = require( '../gui/app-api-gui' )
const admin   = require( './app-api-admin' )
const metrics = require( '../cluster-mgr/pod-metrics' )
// const weblog  = require( './app-weblog.js' )

module.exports = {
  init,
  terminate
}

let svc = null

// ============================================================================

let cfg = {
  API_PARSER_LIMIT: '10mb'
}

async function init( app, configParams ) {
  log.info( 'API init' )
  cfgHlp.setConfig( cfg, configParams )

  svc = app.getExpress()
  svc.use( parser.urlencoded({ limit: cfg.API_PARSER_LIMIT, extended: false }) )
  svc.use( parser.json({ limit: cfg.API_PARSER_LIMIT }) )
  
  svc.use( metrics.apiStats() )

  gui.init( configParams )
  admin.init( configParams )
  db.init( configParams )
  data.init( configParams )

  const secChk   = sec.initSecCheck( app, configParams )
  const adminChk = sec.initAdminCheck( configParams )

  svc.post( '/db',                        adminChk, db.createDB )
  svc.delete( '/db/:db',                  adminChk, db.dropDatabase )

  svc.get(  '/db',                        secChk, db.listDBs )
  svc.post( '/db/:db',                    secChk, db.createCollection )
  svc.get(  '/db/:db/',                   secChk, db.listCollections )
  svc.post( '/db/:db/:coll/index/:field', secChk, db.createIndex )
  svc.delete('/db/:db/:coll/index/:field',secChk, db.dropIndex )
  svc.get(  '/db/:db/:coll/index',        secChk, db.listIndexes )
  svc.delete( '/db/:db/:coll/collection', secChk, db.dropCollection )

  svc.post( '/db/:db/:coll',              secChk, data.insert ) // one or many
  svc.get(  '/db/:db/:coll',              secChk, data.find )   // one or many
  svc.get(  '/db/:db/:coll/count',        secChk, data.countDocuments )
  svc.get(  '/db/:db/:coll/:id',          secChk, data.getDocById )
  svc.put(  '/db/:db/:coll',              secChk, data.update )     // one or many
  svc.put(  '/db/:db/:coll/:id',          secChk, data.replaceOne )
  svc.delete( '/db/:db/:coll',            secChk, data.deleteData ) // one or many
  svc.delete( '/db/:db/:coll/:id',        secChk, data.deleteById ) // one or many

  svc.get(   '/sp',                       adminChk, admin.getSP )
  svc.post(  '/sp',                       adminChk, admin.addSP )
  svc.delete('/sp',                       adminChk, admin.delSP )

  svc.get(   '/monitoring',                adminChk, admin.getMonitoring )
  // svc.post(  '/monitoring',                adminChk, admin.saveMonitoring )
  svc.get(   '/log',                       adminChk, admin.getLogs )
  
  svc.get(  '/admin/user',                adminChk, admin.getUser ) 
  svc.post( '/admin/user',                adminChk, admin.addUser ) 
  svc.post( '/admin/user/:user/autz',     adminChk, admin.addUserAutz ) 
  svc.post( '/admin/user/:user/password', adminChk, admin.changeUserPassword ) 
  svc.delete( '/admin/user/:user/autz',   adminChk, admin.rmUserAutz ) 
  svc.delete( '/admin/user',              adminChk, admin.delUser ) 
  svc.get(  '/admin/config',              adminChk, (req,res) => { res.send( cfgHlp.getAllConfigs() ) } ) 

  svc.get(  '/backup',                    adminChk, db.getBackups )
  svc.post( '/backup',                    adminChk, db.createBackup )
  svc.post( '/restore',                   adminChk, db.restoreBackup )
  svc.get(  '/backup-schedule',           adminChk, db.getBackupSchedule )
  svc.post( '/backup-schedule',           adminChk, db.addBackupSchedule )
  svc.delete( '/backup-schedule',         adminChk, db.delBackupSchedule )
    
  svc.get(  '/cluster/add',              secChk,   cluster.addNode )
  
  svc.get(  '/gui/db/tree',              secChk, gui.getDbTree )
  svc.post( '/gui/db',                   secChk, gui.addDB )
  svc.post( '/gui/coll',                 secChk, gui.addColl )
  svc.post( '/gui/coll/index',           secChk, gui.updateIndex )
  svc.post( '/gui/coll/re-index',        secChk, gui.reIndex )
  svc.get(  '/gui/coll',                 secChk, gui.getCollFrm )
  svc.put(  '/gui/coll',                 secChk, gui.renameColl )
  svc.delete( '/gui/coll',               secChk, gui.delColl )
  svc.get(  '/gui/dbnames',              secChk, gui.getDbNames )
  svc.get(  '/gui/dbnames-all',          secChk, gui.getAllDbNames )
  svc.get(  '/gui/coll/meta',            secChk, gui.getCollMeta )
  svc.get(  '/gui/coll/data',            secChk, gui.getCollData )
  svc.delete('/gui/coll/data',           secChk, gui.delCollData )
  svc.post( '/gui/doc/add',              secChk, gui.addDoc )
  svc.post( '/gui/doc/upload',           secChk, upload(), gui.uploadFile )
  svc.get(  '/gui/doc/add',              secChk, gui.getEmptyDoc )
  svc.post( '/gui/doc/upd',              secChk, gui.updDoc )
  svc.delete('/gui/doc',                 secChk, gui.delDoc )
  svc.get(  '/gui/doc/data',             secChk, gui.getDocData )
  svc.get(  '/gui/toggle-design',        secChk, gui.getToggleDesign )
  svc.get(  '/gui/user',                 secChk, gui.getUser )
  svc.post( '/gui/user',                 secChk, gui.changeUserAutz )
  svc.post( '/gui/user/add',             secChk, gui.addUser )
  svc.delete( '/gui/user',               secChk, gui.delUser )
  svc.get(  '/gui/wiki/:lang/:page',     secChk, gui.wiki )
  svc.get(  '/gui/license/:lang/:page',  secChk, gui.license )
  svc.get(  '/gui/img/:img',             secChk, gui.wikiImg )
  svc.get(  '/gui/auditlog',             secChk, gui.getAuditLog )
  svc.get(  '/swagger.yml',              secChk, gui.getSwagger )
  svc.get(  '/css-custom/custom.css',    secChk, gui.getCSS )
  svc.get(  '/metrics/api',              secChk, metrics.getMetrics )
  svc.get(  '/metrics/db',               secChk, metrics.getDbMetrics )
}

// ============================================================================

async function terminate() {
  log.info( 'Terminate API')
}

// ============================================================================
