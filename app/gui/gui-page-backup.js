const dbBackup = require( '../db-engine/db-backup' )
const { log } = require( '../helper/logger' )

exports: module.exports = {
  addBackupPage
}

// ============================================================================

function addBackupPage( gui ) {
  let backupPg = gui.addPage( 'backupPage', 'Backup', null , null )
  backupPg.navLabel = 'Backup'
  backupPg.title    = 'Backup'
  backupPg.setPageWidth( '90%' )

  gui.pages['backupPage'].dynamicRow( async ( staticRows, req, page ) => {
    let pageCols = []

    const SCHEDULE_COLS = [
      { id: "schedule", label: "Schedule", cellType: "text", width: "12%" },
      { id: "dest",     label: "Destination", cellType: "text", width: "12%" },
      { id: "dbName",   label: "Database",  cellType: "text", width: "15%" },
      { id: "collName", label: "Collection",  cellType: "text", width: "15%" },
      { id: "retention",label: "Retention",   cellType: "text", width: "10%" },
      // { id: "lastBackup",  label: "Last Backup", cellType: "text", width: "10%" },
      { id: "delSchedule", label: "Delete", cellType: "button", method: "DELETE", 
        update: [ { resId: "bcSchedTbl" } ], width:"15%" }
    ]

    pageCols.push( {
      columnId: "backupCol1", width: "40%", height: "700px",
      rows: [
        {
          title: "Backup Schedules",
          id: "bcSchedTbl", rowId: "bcSchedTbl",
          height: "590px", decor: "decor",
          type: "pong-table",
          moduleConfig: {
            dataURL : "",
            rowId   : ['_id'],
            cols    : SCHEDULE_COLS
          },
          resourceURL: "backup-schedule"
        },
        {
          title: "Backup",
          id: "AddBackupFrm", rowId: "AddBackupFrm",
          height: "210px", decor: "decor",
          type: 'pong-form', 
          moduleConfig: {
            fieldGroups: [{
              columns: [
                { formFields: [
                  { id: 'dest', type: 'select', label: 'Destination', 
                    options: [ 
                      { option: 'File' }, 
                      { option: 'Azure Storage BLOB', disabled: true }, 
                      { option: 'AWS S3 bucket', disabled: true }
                    ]},
                  { id: "dbName", type:'select', label: 'DB', editable: true,
                    optionsResource: { resourceURL:"gui/dbnames-all", 
                    optionField: "dbName", optionValue:"dbName" }},
                  { id: "collName", type:'text', label: 'Collection', default: '*' },
                  { id: "schedule", type:'text', label: 'Schedule', default: '0 0 * * *', 
                    descr: 'minute hour dayOfMonth month dayOfWeek' },
                  { id: 'retention', type: 'select', label: 'Retention', 
                    options: [ { option: '1y' }, { option: '3m' },  { option: '1m' },  { option: '1w' }] },
                ] }
              ] 
            }],
            actions : [ 
              { id:'ExecBackup', actionName: 'Execute Backup',
                actionURL: 'backup', target: 'modal', 
                update: [{ resId:'backupsTbl' }] },
              { id:'AddSchedule', actionName: 'Add Schedule',
                actionURL: 'backup-schedule', target: 'modal', 
                update: [{ resId:'bcSchedTbl' }] }
            ]
          },
          resourceURL: "backup"
        }
      ]
    })

    let BACKUP_LOG_COLS =  [
      { id: "date",     label: "Backup Date", cellType: "text", width: "15%" },
      { id: "location", label: "Location",    cellType: "text", width: "10%" },
      { id: "dbName",   label: "Database",    cellType: "text", width: "15%" },
      { id: "collName", label: "Collection",  cellType: "text", width: "15%" },
      { id: "size",     label: "Size",        cellType: "text", width: "7%" },
      { id: "retention",label: "Retention",   cellType: "text", width: "5%" },
      { id: "status",   label: "Status",      cellType: "text", width: "15%" },
      { id: "restore",  label: "",      cellType: "text", width: "10%" }
      // { id: "restore",  label: "Restore", cellType: "link", target: "_parent", 
      //   URL:"index.html?layout=restorePage-nonav",  width:"10%" }
    ]

    pageCols.push({
      columnId: "backupCol2", width: "60%",
      rows: [
        {
          title: "Backups",
          id: "backupsTbl", rowId: "backupsTbl",
          height: "800px", decor: "decor",
          type: "pong-table",
          moduleConfig: {
            dataURL : "",
            pollDataSec: "60",
            rowId   : ['id'],
            cols    : BACKUP_LOG_COLS
          },
          resourceURL: "backup"
        }
      ]
    })

    return [{ 
      id: 'colBackupRow1', rowId: "colBackupRow1",
      // title: 'Dynamic View '+i,
      height: '800px', 
      cols : pageCols
    }]
  })

  // --------------------------------------------------------------------------

  let restorePg = gui.addPage( 'restorePage-nonav', 'Restore Backup', null , null )
  restorePg.title    = 'Restore Backup'
  restorePg.setPageWidth( '90%' )

  gui.pages[ 'restorePage-nonav' ].dynamicRow( async ( staticRows, req, page ) => {
    try {
      log.info( 'restore', req.query )
      let pageCols = []

      let backup = await dbBackup.getBackupById( req.query.id )
      log.debug( 'restore', backup )

      if ( backup._error ) {
        return [{ 
          id: 'colRestoreRow1', rowId: "colRestoreRow1",
          height: '800px', 
          cols : [{

          }]
        }]
      }

      let collFld = { id: "collName", label: 'Collection' }
      if ( backup.doc.collName == '*' ) {
        collFld.type = 'select'
        collFld.options = [ ] //[ { option: '1y' }, { option: '3m' },  { option: '1m' },  { option: '1w' }] },
        if ( backup.doc.collArr ) {
          for ( let collName of backup.doc.collArr ) {
            collFld.options.push({ option: collName })
          }  
        }
      } else {
        collFld.type = 'text'
        collFld.defaultVal = backup.doc.collName
        collFld.readonly = true
      }
      let dtStr =  backup.doc.date.replaceAll('-','').replace(':','')

      pageCols.push( {
        columnId: "backupCol1", width: "50%", height: "700px",
        rows: [
          {
            title: "Restore Backup",
            id: "RestoreFrm", rowId: "RestoreFrm",
            height: "710px", decor: "decor",
            type: 'pong-form', 
            moduleConfig: {
              fieldGroups: [{
                columns: [
                  { formFields: [
                    { id: "_id", type:'text', hidden: true, value: backup.doc._id },
                    { id: "backupDate", type:'text', label: 'Backup Date', readonly: true, defaultVal: backup.doc.date },
                    { id: 'source', type: 'text', label: 'Source', readonly: true, defaultVal: backup.doc.location },
                    { id: "dbName", type:'text', label: 'DB', readonly: true, defaultVal: backup.doc.dbName  },
                    collFld,
                    { id: 'restoreIndex', type: 'checkbox', label: 'Restore Index' },
                    { id: 'deactivateExpire', type: 'checkbox', label: 'Deactivate Expiry' },
                    { id: 'hint', type: 'label', label: 'IMPORTANT: Collection(s() are restored in the database with names &lt;collection-name&gt;-'+dtStr },
                  ] }
                ] 
              }],
              actions : [ 
                { id:'ExecRestore', actionName: 'Restore Backup', actionURL: 'restore', target: 'modal' }
              ]
            },
            resourceURL: "restore"
          }
        ]
      })

      return [{ 
        id: 'colRestoreRow1', rowId: "colRestoreRow1",
        height: '800px', 
        cols : pageCols
      }]
    } catch ( exc ) { log.error( 'restorePg', exc ) }
 })
}
