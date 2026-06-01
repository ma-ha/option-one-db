# Document Attachments

You can attach binary files to documents via REST-API or JS SDK.

Documents must have a unique file name, otherwise the attachment with the same name is replaced.

Attachments should have an `label` field, but you can add any meta data by updating the `_attachment` object in the document.

You can't index or query attachments, but you can query for its meta data of course.

## TL;DR

Full example:

    const { DbClient } = require( 'db-sdk' )
    const { readFile } = require( "node:fs/promises" )

    const dbCredentials = { 
      accessId: process.env.DB_ACCESS_ID
      accessKey: process.env.DB_ACCESS_KEY
    } 
    const client = new DbClient( process.env.DB_URL, dbCredentials )
    await client.connect()
    const db = await client.db( TEST_DB )
    let myAwesomeDocs = await db.collection( 'my-awesome-docs' )

    const fileBuffer = await readFile( 'some-document.pdf' ) // returns a buffer

    const docId = 'a99a63017552332e28badf2e3881fd3e'
    const fileLabel = 'Some document'
    await myAwesomeDocs.attachBLOB( docId, fileLabel, fileBuffer ) 


# Collection

##  async attachFile( docId, label, buffer )

Attach file or update attachment for a document.

Example document with an attachment
    {
      "xy": "Some Product",
      "color": "red",
      "text": "It's an awesome product!",
      "_id": "badf2e388a552332e2899a630171fd3e",
      "_token": "5",
      "_txnId": "UPD.S8E2JF02QP",
      "_cre": 1767980134708,
      "_chg": 1767980134784,
      "_attachment": {
        "barcode.png": {
          "label": "Barcode",
          "_mimetype": "image/png",
          "_size": 1061,
          "_cre": 1767980152080
        }
      }
    }

You can add meta data for the `_attachment`, but ideally don't modify the mimetype, because it can cause trouble in the GUI.

##  async listAttachments( docId ) 

List attachment meta data for a document (returns `_attachment` object)

##  async getAttachment( docId, fileName )

Returns a Buffer with the attached file data.

Example:

    const buffer = await myAwesomeDocs.getAttachments( docId, fileName )
    await writeFile( fileName, buffer )

##  async deleteAttachment( docId, fileName )

Removes the `file` attachment from the specific document.

# Things you might want to know about attachments

1. Attachments are **handled securely as hex buffer**
2. Hex buffers **double the size** of the original file
3. Max attachment file size is limited by RabbitMQ message size. **Max size is 128 MiB**.
4. Huge attachments require fast HW since the response timeout is default 5 sec
