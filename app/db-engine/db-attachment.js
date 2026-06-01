const log     = require( '../helper/logger' ).log
const helper  = require( './db-helper' )
const persistence = require( './db-persistence' )

module.exports = {
  setAttachment,
  getAttachment,
  deleteAttachment
}


async function setAttachment( txnId, dbName, collName, docId, filename, mimetype, size, attachmentHex, label ) {
  log.info( txnId, 'setAttachment', dbName, collName, docId, filename, mimetype, size, label )
  let changedIdxField = []
  // if ( ! doc._token ) {
  //   doc._token = helper.extractToken( doc._id )
  // }
  let docResult = await persistence.getDocById( dbName, collName, docId )
  if ( docResult._error ) { return docResult }
  let doc = docResult.doc
  log.debug( 'setAttachment >>>', doc )
  let attId = await helper.getKeyHash( filename )
  let attResult = await persistence.writeAttachment( txnId, dbName, collName, docId, attId, attachmentHex )
  if ( ! attResult._ok ) { return attResult }

  if ( ! doc._attachment ) { doc._attachment = {} }
  doc._attachment[ filename ] = { 
    label: label,
    _mimetype : mimetype,
    _size : size,
    _cre : (new Date()).toISOString()
  }
  await persistence.updateDocPrep( txnId, dbName, collName, doc )
  let result = await persistence.updateDocCommit( txnId, dbName, collName, docId )

  return result
}


async function getAttachment( txnId, dbName, collName, docId, filename ) {
  log.debug( txnId, 'getAttachment', dbName, collName, docId, filename )
  let docResult = await persistence.getDocById( dbName, collName, docId )
  if ( docResult._error ) { return docResult }
  // let doc = docResult.doc
  // log.info( 'getAttachment >>>', doc )
  let attId = await helper.getKeyHash( filename )
  let attResult = await persistence.readAttachment( txnId, dbName, collName, docId, attId )
  // log.info( 'getAttachment >>>', attResult )
  if ( ! attResult._ok ) {
    return attResult.data
  }
  return attResult
}

async function deleteAttachment( txnId, dbName, collName, docId, filename ) {
  log.info( txnId, 'deleteAttachment', dbName, collName, docId, filename )
  let docResult = await persistence.getDocById( dbName, collName, docId )
  if ( docResult._error ) { return docResult }
  let doc = docResult.doc
  let attId = await helper.getKeyHash( filename )
  let attResult = await persistence.deleteAttachment( txnId, dbName, collName, docId, attId )
  log.info( 'deleteAttachment >>>', attResult )
  if ( attResult._ok ) {

    delete doc._attachment[ filename ]
    if ( Object.keys( doc._attachment ).length == 0 ) {
      delete doc._attachment
    }
    await persistence.updateDocPrep( txnId, dbName, collName, doc )
    await persistence.updateDocCommit( txnId, dbName, collName, docId )
  
  }
  return attResult
}