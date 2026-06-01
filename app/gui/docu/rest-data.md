# Data REST API Reference

In the GUI you can create API access for your app to a database. 

Download the
[OpenAPI reference file](swagger.yml) (aka Swagger)

In the HTTP headers of the REST calls set:
- accessid
- accesskey

## GET /db

List DBs.

Authorization: HTTP header accessid / accesskey 

## POST /db/:db

Create collection

Parameters:
- db: DB name 

Body:
- collection: Collection name 

Authorization: HTTP header accessid / accesskey 

## GET /db/:db/

List collections.

Parameters:
- db: DB name 

Authorization: HTTP header accessid / accesskey 

## POST /db/:db/:coll

Insert one or many documents.

Body:
- doc: Single document or array of documents

Response:
- 202 Accepted
- 400 Bad request
- 500 Server error

Authorization: HTTP header accessid / accesskey 

## GET /db/:db/:coll

Find documents.

Query parameters:
- query: Document filter
  - e.g. `{"abc.z": { "$ge": 0.5 }}`

Response:
- 200 OK
    - body:
        - _ok        : true, 
        - _okCnt     : count
        - _nokCnt    : count
        - docIds     : array of ids
        - data       : array of documents
        - dataLength : byte count
- 400 Bad request
- 500 Server error

Parameters:
- db: DB name 
- coll: Collection name 

Authorization: HTTP header accessid / accesskey 

## GET /db/:db/:coll/count

Count documents.

Query parameters:
- query: Document filter

Parameters:
- db: DB name 
- coll: Collection name 

Authorization: HTTP header accessid / accesskey 

## GET /db/:db/:coll/:id

Get one document by id.

Parameters:
- db: DB name 
- coll: Collection name 
- id: Document _id

Authorization: HTTP header accessid / accesskey 

## PUT /db/:db/:coll

Update one document.

Parameters:
- db: DB name 
- coll: Collection name 

Body:
- filter
- update
- options

Authorization: HTTP header accessid / accesskey 

## PUT /db/:db/:coll/:id

Replace one document.

Parameters:
- db: DB name 
- coll: Collection name 
- id: _id of the document

Body:
- document

Authorization: HTTP header accessid / accesskey 

## DELETE /db/:db/:coll

Delete one or more documents.

Query parameters:
- query: Document filter

Parameters:
- db: DB name 
- coll: Collection name 

Authorization: HTTP header accessid / accesskey 


## POST /db/:db/:coll/:id/attachment

Add an attachment (BLOB) to a document.

Attachments have a file name and an info field. 

Additional meta data can be added by updating the documents `_attachment` objects.

Parameters:
- db: DB name 
- coll: Collection name 
- id: _id of the document

Response:
- 202 Accepted
- 400 Bad request
  - file required
  - parameters not valid
- 401 not authorized
- 500 Server error
  - failed

Authorization: HTTP header accessid / accesskey 

## GET /db/:db/:coll/:id/attachment

Get file names and meta data for all attachments of a document.

Additional meta data can be added or updated by updating the documents `_attachment` objects.

Parameters:
- db: DB name 
- coll: Collection name 
- id: _id of the document

Query parameters:
- label: Text as label for the attachment

Response:
- 200 OK
  - `_attachment` object of the document
- 400 Bad request
  - file required
  - parameters not valid
- 401 not authorized
- 500 Server error
  - failed

Authorization: HTTP header accessid / accesskey 

Example response:

    {
      "barcode.png": {
        "label": "Barcode",
        "_mimetype": "image/png",
        "_size": 1061,
        "_cre": 1767980152080
      }
    }

## GET /db/:db/:coll/:id/attachment/:file

Download an attachment (BLOB) of a specific document.

Parameters:
- db: DB name 
- coll: Collection name 
- id: _id of the document
- file: The filename of the attachments

Response:
- 200 OK
  - streams the file content
- 400 Bad request
  - parameters not valid
- 401 not authorized
- 404 not found
  - doc id not found
  - attachment not found
- 500 Server error
  - failed

Authorization: HTTP header accessid / accesskey 

## DELETE /db/:db/:coll/:id/attachment/:file

Removes the `file` attachment from the specific document.

Parameters:
- db: DB name 
- coll: Collection name 
- id: _id of the document
- file: The filename of the attachments

Response:
- 200 OK
- 400 Bad request
  - parameters not valid
- 401 not authorized
- 404 not found
  - doc id not found
  - attachment not found
- 500 Server error
  - failed

Authorization: HTTP header accessid / accesskey 
