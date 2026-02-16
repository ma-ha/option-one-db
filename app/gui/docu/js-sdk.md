# Class "DbClient"

Here everything starts ...

## TL;DR

Full example:

    const { DbClient } = require( 'db-sdk' )
    const dbCredentials = { 
      accessId: process.env.DB_ACCESS_ID
      accessKey: process.env.DB_ACCESS_KEY
    } 
    const client = new DbClient( process.env.DB_URL, dbCredentials )
    await client.connect()
    const db = await client.db( TEST_DB )
    let myAwesomeDocs = await db.collection( 'my-awesome-docs' )
    let cursor = myAwesomeDocs.find({ name: 'Moe' })
    let docArray = await cursor.toArray()
    for ( let doc of docArray ) {
      console.log( doc )
    }

##  new DbClient( url, [options] )

Constructor

Parameter:
* `url` is a HTTP(S) Endpoint of the DB. 

The `options` should contain the API access credentials:

    { 
      accessId: 'some id', 
      accessKey : 'secret key'
    } 

The API access credentials can be created in the Admin GUI.

You can also use basic auth user/password in the URL.

## async db( dbName )

Connect to DB.s

If the user or API access is with admin rights, the DB will be created, if it does not exist already.

Returns: `Db` object

# Class "Db"

Use `DbClient.db( dbName )` factory to get a `Db` class.

Example:

    const { DbClient } = require( 'db-sdk' )
    const client = new DbClient( 'http://user:password@localhost:9000/db')
    const db = await client.db( 'test-db )

## async createCollection( name, options )

Create a collection.

`options`:
* `primaryKey`: an array of fields used to identify the document uniquely

Example:

    await db.createCollection( 'awesome-data', { primaryKey: ['xz'] } )

## async collection( name, [options] ) 

Connect to a collection.

Returns a `Collection` object.

Example:

    const awesomeColl = await db.collection( 'awesome-data' )

## async collections( [options] ) 

Return: `Array` of all collection names.

## async dropCollection( name, [options] )

Delete a collection permanently.

## dropDatabase( [options] )

Delete a DB permanently.

# Collection

Use `Db.collection( name )` factory to get a `Collection` class.

##  async createIndex( field, [options] ) 

Parameter:
* `field`: String, can be simple or path, e.g. "address.street"
* `options`: all optional
    * `unique`: boolean
    * `expireAfterSeconds`: number
    * `expireAt`: number, Date or date string
    * `msbLength`: number


Returns: `{ ok: true }` or `{ error: "text" }`

##  async listIndexes( [options] ) 

Return: `Array` of `Index` objects.

e.g.

    console.log( await awesomeColl.listIndexes() )

    [ 
      { name: 'PrimaryKey', _PK: [ 'xy' ] }, 
      { name: 'abc', options: {} } ]
    ]

##  async insertOne( doc, [options] ) 

Insert a document. Primary key filed must be available and unique.

##  async insertMany( docArr, [options] ) 

Insert multiple documents. 
If the operation fails, it will rollback all documents.

## find( [filter], [options] ) 

Parameter:
* `filter`

Return: `Cursor` to iterate thru documents.

A filter are structured like this:

    {
      <field1>: <value1>,
      <field2>: { <operator>: <value> },
      <logical operator>: <query>
      ...
    }

Simplest query is for a key and value:

    { <key>: <value> }


Example:

    const awesomeColl = await db.collection( 'awesome-data' )
    const allDocs = await awesomeColl.find({ val: 42 }).toArray()
    for ( const doc of allDocs ) {
      console.log( 'doc:', doc )
    }

### Comparison Operators

- `$eq`   matches values that are equal to a specified value.
- `$gt`   matches values that are greater than a specified value.
- `$ge `  matches values that are greater than or equal to a specified value.
- `$in`   matches any of the values specified in an array.
- `$lt`   matches values that are less than a specified value.
- `$le`   matches values that are less than or equal to a specified value.
- `$ne`   matches all values that are not equal to a specified value.
- `$nin`  matches none of the values specified in an array.
- `$like` matches strings containing the value as substring.

Example:

    myColl.find({ "name": { "$like": "Joe" } })

### Logical Operators

- `$and`   joins query clauses with a logical AND returns all documents that match the conditions of both clauses.
- `$not`   inverts the effect of a query predicate and returns documents that do not match the query predicate.
- `$nor`   joins query clauses with a logical NOR returns all documents that fail to match both clauses.
- `$or`   joins query clauses with a logical OR returns all documents that match the conditions of either clause.

Example:

    myColl.find( {
     $and: [
        { $and: [ 
          { address.zip : { $ge : 40000 } }, 
          { address.zip : { $lt : 50000 } } 
        ] },
        { $or: [ 
          { status: 'Premium' }, 
          { revenue : { $gt : 100000 } } 
        ] }
      ]
    })


##  async findOne( filter, [options] ) 

Find one document by PK or get the first matching document.

##  async findOneAndDelete( filter, [options] ) 

TODO

##  async findOneAndReplace( filter,replacement, [options] ) 

TODO

##  async findOneAndUpdate( filter, update, [options] ) 

TODO

##  async countDocuments( filter, [options] ) 

TODO

##  async replaceOne( doc, [options] ) 

After loading one document from a collection, zou can modify and replace it in the database.

If replacement was success it returns `{ _ok: true, _id: ... }`.

If the PK fields are changed, you will receive an `_error`, because the `_id`. does not match the PK any more.


##  async updateOne( filter, [options] ) 

`filter` can directly address property names and can address sub-properties with a
"path" like `a.b.c`. Supported operations:
* `$set`
* `$unset`
* `$inc`
* `$min`
* `$max`
* `$push` and `{ $push: { <field>: { $each: [ <value1>, <value2> ... ] } } }`
* `$addToSet` also with `$each`
* `$pop`
* `$rename`

TODO

##  async updateMany( filter, update, [options] ) 

see `updateOne`, but filter result can be greater than one.

##  async deleteOne( filter, [options] ) 

see `deleteMany`.. but `deleteOne` fails if filter result is not a single document

##  async deleteMany( filter, [options] ) 

The `filter` can directly address property names and can address sub-properties with a
"path" like `a.b.c`. Supported operations:

# Cursor 

This is returned e.g. by `Collection.find()`.

## async hasNext()

TODO

## async next() 

TODO

## async forEach() 

TODO

## async toArray() 

TODO


[[main.md|Back to index]]