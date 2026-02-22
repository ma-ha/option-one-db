const cfgHlp  = require( '../helper/config' )
const log     = require( '../helper/logger' ).log
const db      = require( '../db-engine/db' )
const admin   = require( '../api/app-api-admin' )
const fs      = require( 'fs' )
const apiHelper = require( '../api/api-helper' )
const manageDB = require( '../db-engine/db-mgr' )

const { httpSatusCodes : st }  = require( '../api/http-codes' )

module.exports = {
  init,

  getDbTree,
  getDbNames,
  getDbNamesSp,
  getAllDbNames,
  getCollMeta,
  getCollData,
  delCollData,

  getEmptyDoc,
  addDoc,
  uploadFile,
  delDoc,
  updDoc,
  getDocData,
  license,
  wiki,
  wikiImg,
  getSwagger,
  getAuditLog,
  getCSS,

  getToggleDesign,
  addUser,
  getUser,
  changeUserAutz,
  delUser,

  addDB, 
  addColl,
  updateIndex,
  reIndex,
  getCollFrm,
  renameColl,
  delColl
}


// ============================================================================

let cfg = {
  GUI_SHOW_USER_MGMT: true,
  GUI_BACKUP_ADMIN: true
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}

// ============================================================================
async function getEmptyDoc( req, res ) {
  try { 
    log.debug( 'getEmptyDoc ...', req.query )
    let  doc = '' 

    if ( req.query.id && req.query.id.indexOf('/') > 0 ) {
      let param =  req.query.id.split('/')
      log.debug( 'manageDB.getColl', param[0], param[1] )
      let coll = await manageDB.getColl( param[0], param[1] )
      let hasIdx = false
      if ( coll.primaryKey != '_id' ) {
        doc += '"'+coll.primaryKey +'": "PK_ChangeMe"'
        hasIdx = 'PK'
      }
      for ( let idx in coll.index ) {
        if ( hasIdx == 'pk' ) { doc += ', ' } 
        else if ( hasIdx == 'PK' ) {
          doc += ',\n  '
          hasIdx == 'idx' 
        }
        doc += '"'+idx + '": "IDX_ChangeMe"'
      }
      let result = {
        id   :req.query.id,
        doc  : '{ '+doc+' }'
      }
      res.send( result )
    } else {
      res.send( { doc: 'ERROR' } )
    }
  } catch ( exc ) {
    log.warn( 'getEmptyDoc', exc )
    res.send( 'Error: '+ exc.message )
  }
}


async function addDoc( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  log.debug( txnId, 'Add Doc ...', req.body )
  try {
    if ( ! req.body.id || req.body.id.indexOf('/') == -1 ) { return res.send( 'id required' ) }
    if ( ! req.body.doc ) { return res.send( 'doc required' ) }
      let param = req.body.id.split('/')
    let dbName   = param[0]
    let collName = param[1]
    let doc = JSON.parse( req.body.doc  )
    log.debug( txnId, 'Add Doc', dbName, collName, doc)
    if ( Array.isArray( doc ) ) {
      let errCnt = 0
      let okCnt = 0
      for ( let oneDoc of doc ) {
        let result = await db.insertOneDoc(
          { 
            db    : dbName, 
            coll  : collName
          },
          oneDoc
        ) 
        log.info( 'Add Doc', result )
        if ( result._error ) { errCnt ++ } else { okCnt ++ }
      }
      return res.send( 'Created: '+okCnt +( errCnt ? ' / Errors: '+errCnt : '') )

    } else {
      let result = await db.insertOneDoc(
        { 
          db    : dbName, 
          coll  : collName
        },
        doc
      ) 
      if ( result._error ) { return res.send( 'ERROR: '+result._error ) }  
      return res.send( 'Created' )
    }
  } catch ( exc ) { 
    log.warn( 'getEmaddDocptyDoc', exc )
    res.send( 'Failed: ' + exc.message ) 
  }
}


async function uploadFile( req, res ) {
  try {
    if ( ! req.files ) { return res.send( 'File not found' ) }
    if ( ! req.body.id ) { return res.send( 'ID required' ) }

    let fileName = req.files.file.name.replace( / /g, '_' )
    log.info( 'uploadFile', fileName, req.body )
    let data = req.files.file.data.toString()
    // log.info( 'uploadFile', data )

    if ( fileName.endsWith( '.csv' ) ) {
      if ( ! req.body.sep ) { return res.send( 'separator required' ) }
      let sep = req.body.sep
      let docArr = []

      var line = data.split( '\n' )
      let field = line[ 0 ].split( sep )
      log.debug( 'uploadFile', field )

      for ( let i = 1; i < line.length; i++ ) {
        let doc = {}
        let value = line[ i ].split( sep )

        function setFld( doc, field, value ) {
          if ( field.indexOf( '.' ) > 0 ) {
            let subfield = field.split( '.' )[0]
            if ( ! doc[ subfield ] ) {
              doc[ subfield ] = {}
            }
            setFld( doc[ subfield ], field.substr( subfield.length + 1 ), value )
          } else {
            doc[ field ] = value
          }
        }

        for ( let j = 0; j < value.length; j++ ) {
          setFld( doc,  field[ j ], value[ j ] )
        }
        log.debug( 'uploadFile', doc )
        docArr.push( doc )
      }

      return res.send({ 
        id  : req.body.id,
        doc: JSON.stringify( docArr, null, '  ' ) 
      }) 
    }

    return res.send({
        id  : req.body.id,
        doc : JSON.stringify( JSON.parse( data ), null, '  ' ) 
      
    }) 

  } catch ( exc ) { 
    log.warn( 'uploadFile', exc )
    res.send({ statusText : 'Failed' }) 
  }
}


async function delDoc( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    if ( ! req.body._id ) { return res.send( 'id required' ) }
    if ( ! req.body.coll || req.body.coll.indexOf('/') == -1 ) { return res.send( 'col required' ) }
    let dbName  = req.body.coll.split('/')[0]
    let colName = req.body.coll.split('/')[1]
    let result = await db.deleteOneDoc( { txnId: txnId, db: dbName, coll: colName }, req.body._id )
    if ( result._error ) { return res.send( 'ERROR: '+result._error ) }
    res.send('Deleted')
  } catch ( exc ) {
    log.warn( 'delDoc', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function updDoc( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  try {
    if ( ! req.body._id ) { return res.send( 'id required' ) }
    if ( ! req.body.doc ) { return res.send( 'doc required' ) }
    if ( ! req.body.coll || req.body.coll.indexOf('/') == -1 ) { return res.send( 'col required' ) }
    let dbName  = req.body.coll.split('/')[0]
    let colName = req.body.coll.split('/')[1]
    let doc = JSON.parse( req.body.doc )
    doc._id  = req.body._id
    doc._cre = req.body._cre
    doc._chg = req.body._chg
    let result = await db.replaceOneDoc( txnId, dbName, colName, req.body._id, doc )
    if ( result._error ) { return res.send( 'ERROR: '+result._error ) }
    res.send('Updated')
  } catch ( exc ) {
    log.warn( 'updDoc', exc.message )
    res.status(400).send( 'Failed: '+  exc.message )
  }
}

async function getDbTree( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  log.debug( txnId, 'Get DB tree ...')
  let tree = {
    // info : "DBs:",
    te : []
  }
  try {
    let dbArr = await manageDB.listDBs( )
    dbArr.sort()
    for ( let dbName of dbArr ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        let dbLeaf = {
          id   : dbName,
          name : 'DB: ' + dbName,
          te : []
        }
        let collArr = await manageDB.getColl( dbName )
        collArr.collections.sort()
        for ( let collName of collArr.collections ) {
          dbLeaf.te.push({ 
            id   : dbName +'/'+ collName,
            name : collName  
          })
        }
        tree.te.push( dbLeaf )
      }
    }      
  } catch ( exc ) {
    log.warn( 'getDbTree', exc.message )
  }
  res.send( tree )
}

async function getDbNames( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  log.info( txnId,  'Get DB names ...')
  let result = []
  try {
    let dbArr = await db.listDBs( )
    for ( let dbName of dbArr ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        result.push({ dbName: dbName })
      }
    }      
  } catch ( exc ) {
    log.warn( 'getDbNames', exc.message )
  }
  res.send( result )
}

async function getDbNamesSp( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  let result = []
  try {
    log.debug( txnId, 'Get DB names for SP...', req.xUserAuthz)
    let dbArr = await db.listDBs( )
    for ( let dbName of dbArr ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        result.push({ dbName: dbName })
      }
    }
    if ( req.xUserAuthz['admin'] ) {
      result.push({ dbName: '*' })
    }
  } catch ( exc ) {
    log.warn( 'getDbNames', exc.message )
  }
  res.send( result )
}

async function getAllDbNames( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId,  'Get DB names all ...')
    let result = []
    let dbArr = []
    if ( cfg.GUI_BACKUP_ADMIN ) {
      dbArr = await db.listAllDBs()
    } else {
      dbArr = await db.listDBs()
    }
    for ( let dbName of dbArr ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        result.push({ dbName: dbName })
      }
    }
    res.send( result )
  } catch ( exc ) {
    log.warn( 'getAllDbNames', exc )
    res.send( [] )
  }
}


// ============================================================================

async function addDB( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId,  'addDB', req.body )
    if ( ! validName( req.body.dbName ) ) {
      return res.status( st.BAD_REQUEST ).send( 'dbName required' )
    }
    if ( await manageDB.getDB( req.body.dbName ) ) {
      return res.send( 'DB already exists' )
    }
    let result = await manageDB.creDB( req.body.dbName )
    db.addAuditLog( req.xUser, 'db', req.body.dbName, 'Add database ', txnId )
  
    log.info( txnId,  'addDB', result)
    if ( result._error ) {
      res.status( st.SERVER_ERROR ).send( result._error+'' )
    } else {
      res.send( 'DB "'+req.body.dbName+'"created: '+result._ok )
    }
  } catch ( exc ) {
    log.warn( 'addDB', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function addColl( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId,  'addColl', req.body  )
    if ( !  validName( req.body.dbName ) || !  validName( req.body.collName ) ) {
      return res.status( st.BAD_REQUEST ).send( 'dbName and collName required' )
    }
    let pk = [ '_id' ]
    if ( req.body.pkFields && req.body.pkFields.trim().length != 0 ) {
      pk = req.body.pkFields.split( ',' ).map( s => s.trim() )
    }
    log.info( 'addCol pk', pk )
    let opts = { pk: pk }
      // if ( pk.length == 0 ) { 
    //   return res.status( st.BAD_REQUEST ).send( 'pkFields required' )
    // }
    if ( ! await manageDB.getDB( req.body.dbName ) ) {
      return res.status( st.BAD_REQUEST ).send( 'DB not found' )
    }
    log.info( 'addCol pdb ok' )
    let idxDef = null
    if ( req.body.idxDef && req.body.idxDef  != '' ) try {
      idxDef = JSON.parse( req.body.idxDef )
      if (typeof idxDef === 'object' ) {
        opts.index = idxDef
      }
    } catch ( exc ) {
      return res.status( st.BAD_REQUEST ).send(  'Add coll idx: '+ exc.message )
    }
  
    let result = await manageDB.creColl( txnId, req.body.dbName, req.body.collName, opts )
    db.addAuditLog( req.xUser, 'db', req.body.dbName+'/'+req.body.collName, 'Add collection', txnId )
  
    log.info( txnId,  'addColl', result)
    if ( result._error ) {
      res.status( st.SERVER_ERROR ).send( result._error+'' )
    } else {
      res.send( 'Collection "'+req.body.dbName+'/'+req.body.collName+'" created: '+result._ok )
    }
  } catch ( exc ) {
    log.warn( 'addColl', exc )
    res.send( 'Error: '+ exc.message )
  }
}


async function updateIndex( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId, 'updateIndex', req.body.name, req.body.idx )
    if ( ! req.body.name || ! req.body.name.indexOf('/') > 0 ) {
      return res.status( st.BAD_REQUEST ).send( 'name as dbName/collName required' )
    }
    let param = req.body.name.split('/')
    let dbName = param[0]
    let collName = param[1]
    let coll = await manageDB.getColl( dbName, collName )
    if ( ! coll ) {
      return res.status( st.BAD_REQUEST ).send( 'collection not found' )
    }
    let idxUpdate = null
    try {
      idxUpdate = JSON.parse( req.body.idx )
      if ( typeof idxUpdate !== 'object' ) {
        idxUpdate = {}
      }
    } catch ( exc ) {
      return res.status( st.BAD_REQUEST ).send( 'Index definition not valid: '+ exc.message )
    }
  
    let creResult = await manageDB.nodesUpdCollIdx( txnId, dbName, collName, idxUpdate )
    for ( let idxName in idxUpdate ) {
      db.addAuditLog( req.xUser, 'db', dbName+'/'+collName+'/'+idxName, 'Add index', txnId )
    }
  
    // log.info( txnId, 'updateIndex', creResult )
    if ( creResult.length == 0 ) {
      return res.send( 'Nothing was changed' )
    }
    return res.send( 'Update index: Done!' )
  } catch ( exc ) {
    log.warn( 'updateIndex', exc )
    res.send( 'Error: '+ exc.message )
  }
}


async function reIndex( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId, 'reIndex', req.body.name, req.body.idx )
    if ( ! req.body.name || ! req.body.name.indexOf('/') > 0 ) {
      return res.status( st.BAD_REQUEST ).send( 'name as dbName/collName required' )
    }
    let param = req.body.name.split('/')
    let dbName = param[0]
    let collName = param[1]
    let coll = await manageDB.getColl( dbName, collName )
    if ( ! coll ) {
      return res.status( st.BAD_REQUEST ).send( 'collection not found' )
    }
    let creResult = await manageDB.reIndexColl( txnId, dbName, collName )
    return res.send( 'Re-index: Done!' )
  } catch ( exc ) {
    log.warn( 'reIndex', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function getCollFrm( req, res ) {
  try {
    let dbArr = await db.listDBs( )
    let dbNameArr = []
    for ( let dbName of dbArr ) {
      if ( req.xUserAuthz['*'] || req.xUserAuthz[ dbName ] ) {
        if ( req.query.dbName && req.query.dbName == dbName ) {
          dbNameArr.push({ dbName: dbName, selected: true })
        } else {
          dbNameArr.push({ dbName: dbName })
        }
      }
    }     
    return res.send({
      collName : 'test',
      dbName: dbNameArr,
      idxDef: '{"name":{}}'
    })

  } catch ( exc ) {
    log.warn( 'renameColl', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function renameColl( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId, 'renameColl', req.body  )
    if ( req.body.id && req.body.id.indexOf('/') > 0 ) {
      if ( req.body.name && req.body.name.indexOf('/') > 0 ) {
        let oldName =  req.body.id.split('/')
        let newName =  req.body.name.split('/')
        if ( oldName[1] != newName[1] ) {
          let coll = await manageDB.getColl( oldName[0], oldName[1] )
          if ( coll ) { // ok, exists
            manageDB.renameColl( txnId, oldName[0], oldName[1], newName[1] )
            db.addAuditLog( req.xUser, 'db', req.body.id , 'rename collection', txnId )
            return res.send( 'Rename initiated, please be patient' )
          }  
        }
      }
    }
    return res.send( 'Rename error: '+req.body.id )
  } catch ( exc ) {
    log.warn( 'renameColl', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function delColl( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId, 'delColl', req.body  )
    if ( req.body.name && req.body.name.indexOf('/') > 0 ) {
      let param =  req.body.name.split('/')
      let coll = await manageDB.getColl( param[0], param[1] )
      if ( coll._error ) { return res.send( 'Collection not found' ) } 
      let delResult = await manageDB.delColl( txnId, param[0], param[1] )
  
      db.addAuditLog( req.xUser, 'db',  req.body.name , 'Delete collection', txnId )
      if ( delResult._ok ) {
        res.send( 'OK, colelction deleted.' )
      } else {
        res.send( 'Error: ' + delResult._error )
      }
    } else {
      res.send( 'name required' )
    }
  } catch ( exc ) {
    log.warn( 'delColl', exc )
    res.send( 'Error: '+ exc.message )
  }
}

// ============================================================================

async function getToggleDesign( req, res ) {
  log.debug( 'toggle design', req.xUser, req.query  )
  let id = await db.getPkID( 'user:'+req.xUser )
  let user = await db.getDocById( 'admin', 'user', id ) // look up in local DB
  if ( ! user || ! user.doc ) {
    return res.redirect( '../index.html?layout=database' )
  }
  let design = 'dark'
  if ( user.doc.design == 'dark' ) {
    design = 'bright'
  }
  await db.updateOneDoc(
    { 
      db     : 'admin', 
      coll   : 'user', 
      update : { $set: { design : design }},
      txnId  : 'ChUsr_' + apiHelper.randomChar( 10 ),
      fn     : 'changeDesign'
    },
    { _id : id },
    { allNodes: true }
  )
  res.redirect( '../index.html?layout=database' )
}

async function addUser( req, res ) {
  await admin.addUser( req, res )
}

async function getUser( req, res ) {
  await admin.getUser( req, res )
}

async function changeUserAutz( req, res ) {
  await admin.changeUserAutz( req, res )
}

async function delUser( req, res ) {
  await admin.delUser( req, res )
}

// ============================================================================

async function getCollMeta( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.debug( txnId,  'getCollMeta ...', req.query )
    let meta = {}
    if ( req.query.id && req.query.id.indexOf('/') > 0 ) {
      let param =  req.query.id.split('/')
      log.debug( txnId, 'manageDB.getColl', param[0], param[1] )
      let coll = await manageDB.getColl( param[0], param[1] )
      log.debug( txnId, 'manageDB.getColl', coll )
      meta.name = req.query.id
      meta.id   = req.query.id
      meta.pk   = ''+ coll.primaryKey 
      meta.idx  = JSON.stringify( coll.index )
    }
    log.debug( txnId,  'getCollMeta meta', meta )
  
    res.send( meta )  
  } catch ( exc ) {
    log.warn( 'getCollMeta', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function getCollData( req, res ) {
  let txnId = db.getTxnId( 'GUI' )
  log.debug( txnId,  'getCollData ...', req.query )
  let tbl = []
  try {
    let dbName = null
    let collName = null
    let qry = null
    if ( req.query.id && req.query.id.indexOf('/') > 0 ) {
      let param =  req.query.id.split('/')
      dbName = param[0]
      collName = param[1]
    } else  if ( req.query.coll && req.query.coll.indexOf('/') > 0 ) {
      let param =  req.query.coll.split('/')
      dbName = param[0]
      collName = param[1]
    } 
    let collMeta = await manageDB.getColl( dbName, collName )
    if ( ! collMeta || collMeta._error ) { 
      res.status(400).send( "Colloection error")
      return tbl 
    }

    let options = { limit: 200 }
    if ( req.query.qry ) {
      try {
        qry = JSON.parse( req.query.qry )
        log.info( 'Query', qry )
      } catch (exc ) { 
        log.warn(  txnId, 'Parse Query', exc.message )
        res.status(400).send( "Query must be JSON: "+exc.message)
        return
      }
    }
    if ( req.query.opts ) {
      try {
        options = JSON.parse( req.query.opts )
        log.info( 'options', options )
      } catch (exc ) { 
        log.warn(  txnId, 'Parse options', exc.message )
        res.status(400).send( "Options must be JSON: "+exc.message)
        return
      }
    }
    let docArr = await db.find( dbName, collName, qry, options )
    log.debug( txnId,  'getCollData', docArr )
    if ( docArr._error ) {
      return res.status(400).send( docArr._error )
    }

    for ( let doc of docArr.data ) {
      log.debug( txnId,  'getCollData',doc)
      let rec = {
        doc  : dbName  +'/'+ collName +'/'+ doc._id,
        _id  : doc._id,
        pk   : '',
        info : ''
      }
      try {
        rec._id =  doc._id.substr(0,5) +'...'+ doc._id.substr(60,64)
      } catch (exc ) { log.warn('getCollData',exc)}
      for ( let pk of collMeta.primaryKey ) {
        if ( rec.pk != '' ) { rec.pk += ', '}
        if ( pk == '_id' ) {
          rec.pk += pk + ': "' + doc[ pk ].substr(0,5) +'...'+ doc[ pk ].substr(60,64)+'"'
        } else {
          rec.pk += pk + ': "' + doc[ pk ] +'"'
        }
      }
      for ( let idx in collMeta.index ) {
        if ( rec.info != '' ) { rec.info += ', '}
        rec.info += idx + ': "' + doc[ idx ] +'"'
      }
      rec.info = extractDocData( doc )
      if ( rec.info?.length > 100 ) {
        rec.info = rec.info.substring( 0, 100 ) + '...'
      }
      tbl.push( rec )
    }
  
  } catch ( exc ) {
    log.warn( 'getCollData', exc )
  }
  res.send( tbl )
}

async function delCollData( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId,  'delCollData ...', req.body )
    for ( let k in req.body ) try {
      let doc = req.body[ k ]
      let dbName = doc.split('/')[0]
      let coll   = doc.split('/')[1]
      let docId  = doc.split('/')[2]
      log.info( txnId,  'delCollData', dbName, coll, docId  )
      await db.deleteOneDoc( { txnId: txnId, db: dbName, coll: coll }, docId )
    } catch ( exc ) { log.warn( 'delCollData', exc.message ) }
    res.send('deleted')
  } catch ( exc ) {
    log.warn( 'delCollData', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function getDocData( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    log.info( txnId, 'getDoclData ...', req.query )
    let dbName = req.query.doc.split('/')[0]
    let coll   = req.query.doc.split('/')[1]
    let docId  = req.query.doc.split('/')[2]
    log.info( txnId, 'getDoclData db, coll, docId', dbName, coll, docId  )
    let result = await db.getDocById( dbName, coll, docId )
    
    if ( result && ! result._error) {
      let rec = {
        coll : dbName +'/'+ coll,
        _id  : docId,
        _cre : ( result.doc._cre ? (new Date( result.doc._cre)).toISOString() : (new Date()).toISOString()),
        _chg : ( result.doc._chg ? (new Date( result.doc._chg)).toISOString() : (new Date()).toISOString()),
      }
      if ( result.doc ) {
        let docCopy = JSON.parse( JSON.stringify( result.doc ) )
        delete docCopy._id
        delete docCopy._token
        delete docCopy._cre
        delete docCopy._chg
        delete docCopy._txnId
        rec.doc = JSON.stringify( docCopy, null, '  ' )  
      } 
  
      res.status( 200 ).send( rec )  
    } else {
      res.status( 200 ).send({})
    }
  } catch ( exc ) {
    log.warn( 'getDoclData', exc.message )
    res.status( 200 ).send({})
  }

}

function extractDocData( doc, format=false ) {
  let docCopy = JSON.parse( JSON.stringify( doc ) )
  delete docCopy._id
  delete docCopy._token
  delete docCopy._cre
  delete docCopy._chg
  delete docCopy._txnId
  if ( format ) {
    return JSON.stringify( docCopy, null, '  ' )
  } else {
    return JSON.stringify( docCopy )  
  }
}

// ============================================================================

async function license( req, res ) {
  try {
    log.debug( 'wiki ...', req.params )
    if ( req.params.page == 'license-note.md') {
      return res.status( 200 ).sendFile( __dirname + '/docu/license-note.md' )
    }
    return res.status( 200 ).sendFile( __dirname + '/docu/license.md' )
  } catch ( exc ) {
    log.warn( 'wiki', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function wiki( req, res ) {
  try {
    log.debug( 'wiki ...', req.params )
    if (  /^[a-z0-9-.]+$/.test( req.params.page ) ) { 
      log.debug( 'wiki ...', __dirname + '/docu/'+ req.params.page )
      if ( req.params.page == 'main.md' ) {
        if ( cfg.GUI_SHOW_USER_MGMT == false ) {
          return res.status( 200 ).sendFile( __dirname + '/docu/main-nu.md' )
        }
      } 
      res.status( 200 ).sendFile( __dirname + '/docu/'+ req.params.page )
    } else {
      res.send( 'invalid page' )
    }
  } catch ( exc ) {
    log.warn( 'wiki', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function wikiImg( req, res ) {
  try {
    log.info( 'wiki img ...', req.params )
    log.info( 'wiki img ...', __dirname + '/docu/'+ req.params.img )
    if ( fs.existsSync(  __dirname + '/docu/'+ req.params.img ) ) {
      res.status( 200 ).sendFile( __dirname + '/docu/'+ req.params.img )
    } else {
      res.send( 'img does not exist' )
    }  
  } catch ( exc ) {
    log.warn( 'wikiImg', exc )
    res.send( 'Error: '+ exc.message )
  }
}



async function getCSS( req, res ) {
  try {
    log.debug( 'CSS', req.xCSS )
    if ( req.xCSS == 'dark' ) {
      res.status( 200 ).sendFile( __dirname + '/css/custom-dark.css' )
    } else {
      res.status( 200 ).sendFile( __dirname + '/css/custom.css' )
    }
  } catch ( exc ) {
    log.warn( 'getCSS', exc )
    res.send( 'Error: '+ exc.message )
  }
}

async function getSwagger( req, res ) {
  try {
    res.status( 200 ).sendFile( __dirname + '/docu/db-swagger.yml' )
  } catch ( exc ) {
    log.warn( 'wikiImg', exc )
    res.send( 'Error: '+ exc.message )
  }
}

// ============================================================================

async function getAuditLog( req, res ) {
  try {
    let txnId = db.getTxnId( 'GUI' )
    // log.info( 'getAuditLog ...', req.params )
    // TODO implement filter 
    log.info( 'getAuditLog ...', req.query.dataFilter )
    let qry =  getDataFilterQuery( req, [ 'dt', 'sp', 'cat' ] ) 
    // log.info( 'getAuditLog ...', qry )
    // let qry = {}
    let docArr = await db.find( 'admin', 'audit-log', qry )
    if ( docArr._error ) { return res.status( st.SERVER_ERROR ).send( docArr._error) }
    // log.info( 'getAuditLog ...', docArr )
    let result = []
    for ( let rec of docArr.data ) {
      result.push({
        ts  : Math.floor( rec.ts / 1000 ),
        cat : rec.cat,
        obj : rec.obj,
        evt : rec.event,
        sp  : rec.sp
      })
      result.sort( (a,b) => { 
        if ( a.ts > b.ts ) return -1
        if ( a.ts < b.ts ) return  1
        return 0
      })
    }
    res.send( result )
  } catch ( exc ) {
    log.warn( 'getAuditLog', exc )
    res.send( 'Error: '+ exc.message )
  }

}

// ============================================================================

function validName( name ) {
  if ( ! name ) { return false }
  if ( ! /^[a-zA-Z0-9-_]+$/.test( name ) ) {
    return false
  } else {
    return true
  }
}

// ============================================================================
// Table filter helper

function getDataFilterQuery( req, params ) {
  let qry = []
  for ( let param of params ) {
    if ( param == 'dt' ) {
      qryDate( req.query.dataFilter[ 'dt' ], qry ) 
    } else {
      let qryForParam = qryDataFilter( req, param )
      if ( qryForParam ) {
        let subQry = {}
        subQry[ param ] = qryForParam
        qry.push( subQry )
      }
    }
  }
  if ( qry.length == 0 ) {
    return null
  } else {
    return { $and: qry }
  }
}

function qryDate( dateStr, qry ) { 
  if (dateStr && dateStr != '') {
    try {
      let startDt = Date.parse( dateStr )
      let endDt   =startDt + 1000*60*60*24
      // log.info( 'qryDate', startDt, endDt )
      qry.push({ ts : { $ge: startDt } })
      qry.push({ ts : { $le: endDt } })
    } catch ( exc ) { log.warn( 'qryDate', exc ) }
  }
}

function qryDataFilter( req, param ) {
  if ( req.query.dataFilter ) {
    if ( req.query.dataFilter[ param ] && req.query.dataFilter[ param ] != '' ) {
      return req.query.dataFilter[ param ]
    }
  }
  return false
}