# JS API: Getting started

Initialize the client:

    const { DbClient } = require( 'db-sdk' )
    const client = new DbClient(
      process.env.DB_URL,
      { accessId: process.env.DB_ACCESS_ID, accessKey: process.env.DB_ACCESS_KEY } 
    )
    await client.connect()

The DB URL is something like `http://localhost:9000/db`.

Access credential can be created in the admin portal in "API Access" tab. If you choose "*" in the database selection, the access is granted to all databases, incl creation of new DBs.

Create or open a DB:

    const db = await client.db( 'test-db' )

Create a Collection:

    await db.createCollection( 'awesome-data', { primaryKey: ['xz'] } )

Insert documented into the collection:

    const awesomeCollection = await db.collection( 'awesome-data' )
    await awesomeCollection.insertOne( { 'xy': xz, abc: 'test', date: Date.now() } )

... see [[js-sdk.md|JS SDK reference]]

Create an index:

    await aw.createIndex( date, {  "msbLen": 16 } ) 

Hint: 
- Dates are stored in ISO format, e.g. `"2025-11-30T05:56:16.410Z"`
- `"msbLen": 16` means, this index just looks at the first 16 characters, e.g. `"2025-11-30T05:56"`

Query using the index:

    let cursor = awesomeCollection.find({ date: { $ge: "2025-11-30T05:56" } })
    let result = await cursor.toArray()

# Naming 

## Databases and collections

Only letters "A".."Z,"a".."z" and "-" are allowed for databases and collections names

"admin" is a reserved DB name.

Upper case letters are also allowed, but the convention is to use all lower case names.

Even the minimum name length of one character is valid, it's recommended to use "good" self explaining names.

## Reserved property names

In documents the following property names are reserved (and are auto-generated):
* `_id` which is generated as SHA256 based hash of the primary key
* `_token` which is the first letters of the `_id` 
* `_cre` create date of the document in the DB
* `_chg` update date of the document in the DB
* `_txnId` for replication consistency check
* `_keyVal`inside key properties

# Query Documents

see [[query.md|How to query documents]]
