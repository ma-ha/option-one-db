const db = require( '../db-engine/db' )

exports: module.exports = {
  addUserPage,
  addAuditLogPage
}

// ============================================================================

function addUserPage( gui, pwdHint ) {
  let userPg = gui.addPage( 'userPage', 'Pager', null , null )
  userPg.navLabel = 'User'
  userPg.title    = 'User'
  userPg.setPageWidth( '90%' )

  gui.pages['userPage'].dynamicRow( async ( staticRows, req, page ) => {

    let cols =  [
      { id: "_userid", label: "User ID", cellType: "text", width: "10%" },
      { id: "_email", label: "E-Mail",   cellType: "text", width: "10%" },
      { id: "delUser", label: "Delete User", cellType: "button", method: "DELETE", 
        update: [ { resId: "userAdminTbl" } ], width:"10%" },
      { id: "_admin", label: "Admin", cellType: "checkbox", editable: "true", width: "10%" }
    ]

    let dbArr = await db.listDBs(  )
    let colFillWIdth = '10%'
    switch ( dbArr.length ) {
      case 2: colFillWIdth = '60%'; break;
      case 3: colFillWIdth = '50%'; break;
      case 4: colFillWIdth = '40%'; break;
      case 5: colFillWIdth = '30%'; break;    
      case 6: colFillWIdth = '20%'; break;    
      default: break;
    }
    for ( let dbName of dbArr ) {
      cols.push({ id: dbName, label: dbName, cellType: "checkbox", editable: "true", width: '10%' })
    }

    cols.push({ id: "_all", label: "All DBs", cellType: "checkbox", editable: "true", width: colFillWIdth })

    let tblView = {
      title: "User Admin",
      id: "userAdminTbl", rowId: "userAdminTbl",
      height: "650px", decor: "decor",
      type: "pong-table",
      moduleConfig: {
        dataURL : "",
        pollDataSec: "15",
        rowId   : "_userid",
        cols    : cols
      },
      resourceURL: "gui/user"
    }

    let addUserForm = {
      title: 'Add User',
      id: 'addUser', rowId: 'addUser',
      height: '160px', decor: 'decor',
      type: 'pong-form', 
      moduleConfig: {
        fieldGroups: [{
          columns: [
            { formFields: [
              { id: 'user',     type: 'text',     label: 'User ID' },
              { id: 'email',    type: 'text',     label: 'E-Mail',  },
              { id: 'password', type: 'password', label: 'Password' },
              { id: 'hint', type:'label', label: pwdHint }
            ] }
          ] 
        }],
        actions : [ 
          { id:'AddUser', actionName: 'Add User',  update: [{ resId:'userAdminTbl' }],
            actionURL: 'gui/user/add', target: 'modal'  }
        ]
      },
      resourceURL: 'gui/doc/data'
    }
    return [ tblView, addUserForm ]
  })
}


// ============================================================================

function addAuditLogPage( gui ) {
  let userPg = gui.addPage( 'AuditLog', 'Audit Log',  
    {
      title: "Audit Log",
      id: "auditLog", 
      height: "790px", decor: "decor",
      type: "pong-table",
      pollDataSec: "5",
      resourceURL: "gui/auditlog"
    }, 
    {
      dataURL: "",
      filter:{
        dataReqParams: [ 
          { id:'cat', label:'Category', type:'text' },
          { id:'sp', label:'By', type:'text' },
          { id:'dt', label:' Date', type: 'date' }
        ],
        dataReqParamsSrc: 'Form'
      },
      cols: [
        { id: "ts",  label: "Date", cellType: "date", width:"10%", },
        { id: "sp",  label: "By ", cellType: "text", width: "10%" },
        { id: "cat", label: "Category", cellType: "text", width: "10%" },
        { id: "obj", label: "Object", cellType: "text", width: "20%" },
        { id: "evt", label: "Event", cellType: "text", width: "50%" }
      ]
    } 
  )
  userPg.navLabel = 'AuditLog'
  userPg.title    = 'AuditLog'
  userPg.setPageWidth( '90%' )
}