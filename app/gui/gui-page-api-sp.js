
exports: module.exports = {
  addApiSpPage
}

// ============================================================================

function addApiSpPage( gui ) {
  let apiSpPg = gui.addPage( 'apiSpPage', 'API', null , null )
  apiSpPg.navLabel = 'API Access'
  apiSpPg.title    = 'API Access'
  apiSpPg.setPageWidth( '90%' )

  gui.pages['apiSpPage'].dynamicRow( async ( staticRows, req, page ) => {

    let cols =  [
      { id: "db",      label: "Database",  cellType: "text", width: "15%" },
      { id: "app",     label: "App Name",  cellType: "text", width: "15%" },
      { id: "accId",   label: "Access Id", cellType: "text", width: "15%" },
      { id: "access",  label: "Access",    cellType: "text", width: "15%" },
      { id: "expires", label: "Expires",   cellType: "text", width: "15%" },
      { id: "delApp",  label: "Deactivate", cellType: "button", method: "DELETE", target: 'modal',
        update: [ { resId: "spTbl" } ], width:"10%" }
    ]

    let tblView = {
      title: "API Access",
      id: "spTbl", rowId: "spTbl",
      height: "550px", decor: "decor",
      type: "pong-table",
      moduleConfig: {
        dataURL : "",
        pollDataSec: "60",
        rowId   : ['id','db'],
        cols    : cols
      },
      resourceURL: "sp"
    }

    let addUserForm = {
      title: 'Add API Access',
      id: 'addSp', rowId: 'addSp',
      height: '260px', decor: 'decor',
      type: 'pong-form', 
      moduleConfig: {
        fieldGroups: [{
          columns: [
            { formFields: [
              { id: 'db',     type: 'select', label: 'Database',  optionsResource: { resourceURL: 'gui/dbnames', optionField:'dbName', optionValue:'dbName'} },
              { id: 'app',    type: 'text',   label: 'App Name',  },
              { id: 'access', type: 'select', label: 'Access', 
                options: [ { option: 'Database API' }, { option: 'Admin API' }] },
              { id: 'expires', type: 'select', label: 'Expires', 
                options: [ { option: '3m' }, { option: '1y' }, { option: 'never' } ] }
            ] }
          ] 
        }],
        actions : [ 
          { id:'AddSp', actionName: 'Add API Access',  update: [{ resId:'spTbl' }],
            actionURL: 'sp', target: 'modal'  }
        ]
      },
      resourceURL: 'sp'
    }
    return [ tblView, addUserForm ]
  })
}
