const log     = require( '../helper/logger' ).log
const cron    = require( 'node-cron' )
const db      = require( '../db-engine/db' )
const dbFile  = require( '../db-engine/db-file' )
const helper  = require( './db-helper' )
const pubsub  = require( '../cluster-mgr/pubsub' )

module.exports = {
  init,
  processJob,
  startSchedule,
  cancelBackupSchedule,
  getBackups,
  getBackupById,
  createBackup,
  runBackupOnNode,
  getBackupSchedule,
  addBackupSchedule,
  delBackupSchedule,
  startRestoreJob
}

// ============================================================================
let NODE_ID = ''

async function init( nodeId = '' ) {
  log.info( 'BACKUP init ...' )
  try {
    NODE_ID = nodeId.replaceAll('/','').replaceAll(':','').replaceAll('.','').replaceAll('_','')
    let backupSchedule = await getBackupSchedule()
    for ( let schedule of backupSchedule.doc ) {
      startSchedule( schedule )
    }
    setInterval( deleteExpiredBackups, 3*60*60000 ) // schedule every 3 h
    setTimeout(  deleteExpiredBackups, 5000 )      // clean up 1 min init
  } catch ( exc ) {
    log.error( 'BACKUP init', exc )
  }
}

async function processJob( job ) {
  log.info( 'BACKUP: Process job', job?.task?.op )
  let tsk = job.task
  switch ( job.task.op ) {

    case 'new backup schedule':
      await startSchedule( tsk.backup )
      break

    case 'delete backup schedule':
      await cancelBackupSchedule(  tsk.backupId  )
      break

    case 'create backup':
      await runBackupOnNode( tsk.backup )
      break

    case 'restore backup':
      await restoreBackupOnNode( tsk.restore )
      break
  }
}

// ============================================================================

let activeCrons = {}

async function startSchedule( backup ) {
  log.info( 'BACKUP add schedule', backup.schedule, backup.dbName, backup.collName )
  try {
    let cronSchedule = cron.schedule(
      backup.schedule,
      async () => {
        runBackupOnNode( backup )
      }, 
      { noOverlap: true }
    )
    activeCrons[ backup._id ] = cronSchedule
  } catch ( exc ) { log.error( 'startSchedule', backup, exc )}
}


async function cancelBackupSchedule( backupId ) {
  log.warn( 'cancelBackupSchedule', backupId )
  try {
    activeCrons[ backupId ].destroy();
  } catch ( exc ) {
    log.warn( 'cancelBackupSchedule', backupId, exc )
  }
}

// ============================================================================

async function collBackupArr( dbName, collName )  {
  let collArr = []
  if ( collName == '*' ) {
    let dbColl = await db.getColl( dbName )
    for ( let coll of dbColl.collections ) {
      collArr.push( coll)
    }
  } else {
    collArr.push( collName )
  }
  return collArr
}

async function addStartRec( dateStr, backup, collName, collArr ) {
  try {
    let backupId = ( backup._id ? backup._id : 'exec') + dateStr.replaceAll('-','').replaceAll('T','').replaceAll(':','')
    let result = await db.insertOneDoc(
      { db: 'admin', coll: 'backup' },
      {
        _id      : ( backupId + NODE_ID ).toLowerCase(),
        backupId : backupId,
        nodeId   : NODE_ID,
        date     : dateStr,
        dbName   : backup.dbName,
        location : backup.dest,
        collName : ( collName ? collName : '*' ),
        collArr  : collArr,
        retention: backup.retention,
        status   : 'Started',
        size     : '?'
      }
    )
    log.debug( 'addStartRec', result )
    return backupId
  } catch ( exc ) { 
    log.error( 'addStartRec', exc ) 
    return { _error: 'Add start failed' }
  }
}

async function updateRec( backupId, result ) {
  try {
    log.debug( 'updateRec', backupId,  result )
    await db.updateOneDoc(
      { db: 'admin', coll: 'backup',
        update: {
          '$set'   : {
            status   : result.status,
            size     : result.size
          }  
        } 
      },
      { _id : ( backupId + NODE_ID).toLowerCase() }
    )
    return backupId
  } catch ( exc ) { 
    log.error( 'updateRec', exc ) 
    return { _error: 'Update failed' }
  }
}


// ============================================================================

async function getBackups() {
  return await dbFile.loadAllDoc( 'admin', 'backup', null, { limit: 1000 } )
}

async function getBackupById( id ) {
  return await dbFile.loadDocById( 'admin', 'backup', id )
}

async function getBackupSchedule() {
  return await dbFile.loadAllDoc( 'admin', 'backup-schedule' )
}

async function addBackupSchedule( schedule, destination, dbName, collName, retention ) {
  try {
    let backup = {
      schedule   : schedule,
      dbName     : dbName,
      dest       : destination,
      collName   : ( collName ? collName : '*' ),
      retention  : retention,
      lastBackup : ''
    }
    let result = await db.insertOneDoc( { db: 'admin', coll: 'backup-schedule' },backup  )
    await pubsub.sendBackupOp( backupJobID(), 'new backup schedule', { backup: backup }  )

    return result
  } catch ( exc ) { 
    log.error( 'addBackupSchedule', exc ) 
    return { _error: 'Delete failed' }
  }
}

async function delBackupSchedule( id ) {
  log.info( 'delBackupSchedule', id ) 
  try {
    let result = await db.deleteOneDoc(
      { 
        db    : 'admin',
        coll  : 'backup-schedule'
      },
      id
    )
    await pubsub.sendBackupOp( backupJobID(), 'delete backup schedule', { backupId: id } )

    return result
  } catch ( exc ) {
    log.error( 'delBackupSchedule', exc ) 
    return { _error: 'Delete failed' }
  }
}


async function runBackupOnNode( backup ) {
  try {
    log.info( 'runBackupOnNode', backup.dbName, backup.collName, backup.retention )
    let dateStr = ( new Date() ).toISOString().substring( 0, 16 )
    const collectionsToBackup = await collBackupArr(  backup.dbName, backup.collName )
    let rec = await addStartRec( dateStr, backup, backup.collName, collectionsToBackup )
    let backupResult = {
      status: 'OK',
      size  : 0
    } 
    for ( const collName of collectionsToBackup ) {
      if ( backup.dest == 'File' ) {
        let result = await dbFile.creBackup( dateStr,  backup.dbName, collName )
        log.info( 'runBackupOnNode result', result )

        if ( ! result._ok ) {
          backupResult.status = 'FAILED'
        } else {
          backupResult.size += result.size
        }
      } else {
        log.warn( 'BACKUP skipped (unsupported destination):', backup)
      }
    } 
    backupResult.size = (Math.round( backupResult.size * 100 ) / 100 )+ ' MB'
    await updateRec( rec, backupResult )
  } catch ( exc ) { log.error( 'runBackupOnNode', backup, exc ) }
}


async function createBackup( destination, dbName, collName, retention ) {
  log.info( 'createBackup', destination, dbName, collName, retention )
  await pubsub.sendBackupOp(
    backupJobID(),
    'create backup',
    { backup: {
        dest      : destination,
        dbName    : dbName,
        collName  : collName,
        retention : retention
      }
    }
  )
}

async function startRestoreJob( date, source, dbName, collName, restoreIndex, deactivateExpire ) {
  let restoreJob =  {
    date      : date,
    source    : source,
    dbName    : dbName,
    collName  : collName,
    restoreIndex     : restoreIndex,
    deactivateExpire : deactivateExpire
  }
  log.info( 'restoreJob', restoreJob )
  await pubsub.sendBackupOp(
    backupJobID(),
    'restore backup',
    { restore: restoreJob }
  )
}

async function restoreBackupOnNode( restore ) {
  try {
    log.info( 'restoreBackupOnNode', restore )
    if ( restore.source == 'File' ) {

      let result = await dbFile.restoreBackup( 
        restore.date,
        restore.dbName,
        restore.collName,
        restore.restoreIndex,
        restore.deactivateExpire
      )
      log.info( 'restoreBackupOnNode result', result )

    } else {
      log.warn( 'restoreBackupOnNode skipped (unsupported source):', restore)
    }

  } catch ( exc ) { log.error( 'restoreBackupOnNode', restore, exc ) }
}

// ============================================================================

function backupJobID() {
  return 'BACK.'+ helper.randomChar( 10 )
}
// ============================================================================

async function deleteExpiredBackups() {
  log.info( 'deleteExpiredBackups...' )
  try {
    let backups = await dbFile.loadAllDoc( 'admin', 'backup', null, { limit: 1000 } )
    if ( ! backups._ok ) { log.error( 'deleteExpiredBackups', backups ) }
    for ( let backup of backups.doc  ) {
      if ( backup.status != 'OK' ) {
        if ( cleanUpOldBackupData( backup ) ) {
          log.debug( 'deleteExpiredBackups, delete rec', backup )
          await db.deleteOneDoc( { db: 'admin', coll: 'backup'}, backup._id )
        }
        continue
      }
      let expire = Date.now()
      switch ( backup.retention ) {
        case '1y': expire -= 365 * 24 * 60 * 60000; break
        case '3m': expire -= 90 * 24 * 60 * 60000; break
        case '1m': expire -= 31 * 24 * 60 * 60000; break
        case '1w': expire -= 7 * 24 * 60 * 60000; break
        default: expire -= 365 * 24 * 60 * 60000; break
      }
      if ( backup._cre < expire ) {
        log.info( 'deleteExpiredBackups', backup._id, backup.date, backup.retention)
        await dbFile.purgeBackup( backup.date, backup.dbName, backup.collArr )
        await db.updateOneDoc(
          { db: 'admin', coll: 'backup',
            update: {
              '$set'   : {
                status   : 'ERASED',
                size     : '0'
              }  
            } 
          },
          { _id : backup._id }
        )
      }
    }
  } catch ( exc ) {
    log.error( 'deleteExpiredBackups', exc )
    
  }
}

function cleanUpOldBackupData( backup ) {
  let showDt =  24*60*60000 // 1d
  switch ( backup.retention ) { // hide too old deleted backup
    case '1w' : showDt = showDt *  14; break
    case '1m' : showDt = showDt *  60; break
    case '3m' : showDt = showDt * 180; break
    case '1y' : showDt = showDt * 180; break
  }
  if (  Date.now() - backup._chg > showDt ) { return true } 
  return false
}