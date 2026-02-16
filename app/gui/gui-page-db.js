const { log } = require( "../helper/logger" )

exports: module.exports = {
  addDatabasePage,
  addScriptPage
}

// ============================================================================

function addDatabasePage( gui, showAddDB ) {

  let userPg = gui.addPage( 'database', 'Pager', null , null )
  userPg.navLabel = 'Database'
  userPg.title    = 'Database'
  userPg.setPageWidth( '90%' )

  gui.pages['database'].dynamicRow( async ( staticRows, req, page ) => {
      let userid = await gui.getUserIdFromReq( req )
      if ( ! userid ) {

        return [{ 
          id     : 'colRow1', 
          height : '800px', 
          decor  : "decor",
          resourceURL: "gui/static/welcome"        
        }]

      } else {
        
        let cols = []

        const DB_TREE = {
          title: "Database Structure", id: "dbTree", rowId: "dbTree", 
          type: "pong-tree",  resourceURL: "gui/db/tree",
          height: "454px",decor: "decor",
          moduleConfig: {
            idField: "id",
            dataURL: "gui/db/tree",
            titleField: "info",
            treeArray: "te",
            labelField: "name",
            pollDataSec: "20",
            maxDeepth: "3",
            update: [
              "collDesc",
              "collDocList",
              "docAdd",
              "collQry"
            ]
          }
        }

        const ADD_DB_FORM = {
          title: "Add DB",
          id: "AddDbForm", rowId: "AddDbForm",
          height: "120px", decor: "decor",
          type: 'pong-form', 
          moduleConfig: {
            fieldGroups: [{
              columns: [
                { formFields: [
                  { id: "dbName", type:'text', label: 'DB Name' }
                ] }
              ] 
            }],
            actions : [ 
              { id:'CreateDB', actionName: 'Create Database',
                actionURL: 'gui/db', target: 'modal', 
                update: [{ resId:'dbTree' },{ resId:'AddCollForm' }] }
            ]
          },
          resourceURL: "gui/db"
        }

        const ADD_COLL_FORM = { title: "Add Collection",
          id: "AddCollForm", rowId: "AddCollForm",
          height: "225px", decor: "decor",
          type: 'pong-form', 
          moduleConfig: {
            fieldGroups: [{
              columns: [
                { formFields: [
                  { id: "dbName", type:'select', label: 'DB', editable: true,
                    optionsResource: { resourceURL:"gui/dbnames", 
                    optionField: "dbName", optionValue:"dbName" }  },
                  { id: "collName", type:'text', label: 'Collection' },
                  { id: "pkFields", type:'text', label: 'Primary Key', descr: 'Comma separated field names, leave empty if an _id should be generated.' },
                  { id: "idxDef",  type:'text', label: 'Index', descr: 'JSON map of index name and index definitions, e.g. {"name":{}}' }
                ] }
              ] 
            }],
            actions : [ 
              { id:'CreColl', actionName: 'Create Collection',
                actionURL: 'gui/coll', target: 'modal',
                update: [{ resId:'dbTree' }] }
            ]
          },
          resourceURL: "gui/coll"
        }
        
        const COLL1 = {
          columnId: "col1", width: "20%", height: "700px",
          rows: [ ]
        }

        if ( showAddDB ) {
          COLL1.rows = [ DB_TREE, ADD_DB_FORM, ADD_COLL_FORM ]
        } else {
          DB_TREE.height = '620px'
          COLL1.rows = [ DB_TREE, ADD_COLL_FORM ]
        }

        cols.push( COLL1 )
        cols.push({
          columnId: "col2", width: "50%",
          rows: [
            {
              title: "Collection",
              id: "collDesc", rowId: "collDesc", 
              height: "auto", decor: "decor",
              resourceURL: 'gui/coll/meta',
              type: 'pong-form', 
              moduleConfig: {
                fieldGroups: [{
                  columns: [
                    { formFields: [
                      { id: "id",   type:'text', hidden: true },
                      { id: "name", type:'text', label: 'Collection' },
                      { id: "pk",   type:'text', label: 'PK', readonly: true },
                      { id: "idx",  type:'text', label: 'Index', descr: 'JSON map of index name and index definitions, e.g.{"startDate":{"msbLen":16}}' }
                    ]} ] 
                }],
                actions : [ 
                  { id:'UpdCollIdx', actionName: 'Update Index',
                    modalQuestion: 'Really update index?',
                    actionURL: 'gui/coll/index', method: 'POST', target: 'modal' },
                  { id:'ReIndexColl', actionName: 'Re/Index',
                    modalQuestion: 'Really re-index column?',
                    actionURL: 'gui/coll/re-index', method: 'POST', target: 'modal' },
                  { id:'DelColl', actionName: 'Delete Collection',
                    modalQuestion: 'Really delete the collection?',
                    actionURL: 'gui/coll', method: 'DELETE', target: 'modal',
                    update: [{ resId:'dbTree' }] },
                  { id:'RenamelColl', actionName: 'Rename Collection',
                    modalQuestion: 'Really rename the collection?',
                    actionURL: 'gui/coll', method: 'PUT', target: 'modal',
                    update: [{ resId:'dbTree' }] },
                  { id: "cascadeUpdate", afterUpdate:"*", 
                    setData: [ { resId:"docUpload" } ] } 
                ]
              }
            },
            {
              title: "Query",
              id: "collQry", rowId: "collQry", 
              height: "120px", decor: "decor",
              resourceURL: 'gui/coll/meta',
              type: 'pong-form', 
              moduleConfig: {
                fieldGroups: [{
                  columns: [
                    { formFields: [
                      { id: "qry", type:'text', label: 'Query', rows:2, 
                        descr: "e.g. { ''name'': ''Joe'' } or { ''name'': { ''$like'': ''Joe'' } } or ..." },
                      { id: "opts", type:'text', label: 'Options', descr: "e.g. { ''MAX_ID_SCAN'': 100000, ''limit'': 200 }" },
                      { id: "id",  type:'text', hidden:true },
                    ]} ] 
                }],
                actions : [ 
                  { id:'QryBtn', actionName: 'Execute Query',
                    actionURL: "gui/coll/data", method: 'GET',
                    setData: [{ resId:'collDocList' }] }
                ]
              }
            },
            {
              title: "Documents",
              id: "collDocList", rowId: "collDocList",
              height: "540px", decor: "decor",
              type: "pong-table",
              moduleConfig: {
                rowId: "doc",
                cols: [
                  { id: "show", label: "Show", cellType: "button", width:"5%", 
                    method: 'GET', update: [{ resId: 'docEdit' }  ], icon: 'ui-icon-pencil' },
                  { id: "sel",  label: "", cellType: "selector", width: "5%"},
                  { id: "_id",  label: "ID", cellType: "text", width: "10%" },
                  { id: "pk",   label: "PK", cellType: "text", width: "15%" },
                  { id: "info", label: "Data", cellType: "text", width: "90%" }
                ],
                actions: [
                  {
                      id: "delDocs",
                      actionName: "Delete Documents",
                      modalQuestion:"Do you really want to delete the selected documents?",
                      method: "DELETE",
                      actionURL: "gui/coll/data",
                      paramLstName: "docs",
                      params: [
                        { name:"id", value:"${doc}"  }
                      ],
                      execute: ["collQryContentBtQryBtn"], 
                        // params: [
		                    //   { name: "id", value: "${doc}" }
		                    // ] 
                      // }]
                  }
                ]
              },
              resourceURL: "gui/coll/data"
            }
          ]
        })

        cols.push({
          columnId: "col3", width: "30%",
          rows: [
            {
              title: "Document",
              id: "docEdit", rowId: "docEdit",
              height: "430px", decor: "decor",
              type: 'pong-form', 
              moduleConfig: {
                fieldGroups: [{
                  columns: [
                    { formFields: [
                      { id: "coll", type:'text', label: 'Collection', hidden: true },
                      { id: "_id", type:'text', label: 'ID', readonly: true },
                      { id: "_cre", type:'text', label: 'Create Date', readonly: true },
                      { id: "_chg", type:'text', label: 'Change Date', readonly: true },
                      { id: "doc", type:'text', label: 'JSON', rows:'13' },
                    ] }
                  ] 
                }],
                actions : [ 
                  { id:'UpdDoc', actionName: 'Update',
                    modalQuestion:"Do you really want to change this document?",
                    actionURL: 'gui/doc/upd', target: 'modal', update: [{ resId:'collDocList' }]  },
                  { id:'DeldDoc', actionName: 'Delete', method: 'DELETE',
                    modalQuestion:"Do you really want to delete this document?",
                    actionURL: 'gui/doc', target: 'modal' ,update: [{ resId:'collDocList' }]   }
                ]
              },
              resourceURL: "gui/doc/data"
            },
            {
              title: "Add Document(s)",
              id: "docAdd", rowId: "docAdd",
              height: "270px", decor: "decor",
              type: 'pong-form',         
              moduleConfig: {
                fieldGroups: [{
                  columns: [{ formFields: [
                    { id: "id",   type:'text', label: 'DB/Coll',  hidden: true },
                    { id: "doc",  type:'text', label: 'JSON', rows:'12' },
                  ] }] 
                }],
                actions : [ 
                  { id:'AddDoc', actionName: 'Add', actionURL: 'gui/doc/add',
                   target: 'modal', update: [{ resId:'collDocList' }] }
                ]
              },
              resourceURL: "gui/doc/add"
            },
            {
              title: "Upload CSV/JSON",
              id: "docUpload", rowId: "docUpload",
              height: "100px", decor: "decor",
              type: "pong-upload",
              resourceURL: "gui/doc/upload",
              moduleConfig: {
                setData : [ "docAdd" ],
                input: [
                  { id:"id", name:"id", hidden: true },
                  { id:"sep", name:"sep", label: "CSV Separator", value:";" }
                ], 
                accept: ".json,.csv"
              }
            }
          ]
        })

  
        return [{ 
          id: 'colRow1', rowId: "colRow1",
          // title: 'Dynamic View '+i,
          height: '800px', 
          cols : cols
        }]
      }

    })
}

// ============================================================================


function addScriptPage( gui ) {
  // let userPg = gui.addPage( 'scriptPage', 'Script',  { id:'scriptShell' }, null )
  // userPg.navLabel = 'Script'
  // userPg.title    = 'Script'
  // userPg.setPageWidth( '90%' )
}
