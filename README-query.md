# Query Documents

Queries can be done on any document field or sub-field.

Fastest is retrieval od one document by `_id` or primary key.

Queries on indexed fields help to get a result quickly.

A query are structured like this:

    query = {
      <field1>: <value1>,
      <field2>: { <operator>: <value> },
      <logical operator>: <query>
      ...
    }

Simplest query is for a key and value:

    query = { <key>: <value> }


Example:

    query = { "name": "Joe" }


JS SDK usage:

    const client = new DbClient( DB_URL )
    await client.connect()
    const db = await client.db( TEST_DB )
    const myColl = await db.collection( 'my-collection' )
    let cursor = myColl.find( query, projection, options )
    let resultArr = await cursor.toArray()

Projection is an array of field names, e.g.:

    projection = [ 'cust-no', 'name', 'address.city' ]

Supported option:

- `options.optimize ='only master nodes'` 
   this gets around the quorum in full collection scans: Each node only query its master token documents and skips replica. This reduces I/O, CPU and communication for the data nodes and less documents (no replica docs) to consolidate at the API nodes.

Example:

    let cursor = customer.find( { address.zip: 12345 }, [ 'custNo', 'name', 'address' ], { optimize: 'only master nodes } )
    let customerIn12345 = await cursor.toArray()

The `customerIn12345` may look like:

    [
      {
        _id : 450ec2c571b6ae715b98cf73063ae10304da65f5b072b34d0a5d2c1aded123c7,
        custNo: 123456,
        name: 'John Doe', 
        address: {
          city: 'Moetown',
          zip: 12345,
          street: 'Main Street 134'
        }
      }, 
      {
        ...
      }
    ]

*WARNING: The projection returns always the `_id`, but not the PK fields ... unless you request them explicitly.*

# Comparison Operators

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

# Logical Operators

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

# Element Operators

- `$exists`  matches documents that have the specified field.
- `$type`    selects documents if a field is of the specified type.

# Field Indexes

Creating indexes help avoiding full scan thru all documents for queries.

- Fastest: Queries contains only index properties. Only index is used to retrieve the result.
- Fast: Queries contain at least one indexed property. This helps to reduce the document candidates. Only this short list from index must be scanned for the non/indexed field queries.

Please use the Admin GUI if you need to re-build the index (not in SDK or API).

## Index option "msbLen"

Example: Create an index for the `date` field:

    await myColl.createIndex( 'date', { msbLen: 16 } ) 

Dates are stored in ISO format, e.g. `"2025-11-30T05:56:16.410Z"`

`"msbLen": 16` indicates, this index just looks at the first 16 characters, e.g. `"2025-11-30T05:56"` and ignores the least significant bytes.

## Index option "expiresAfterSeconds"

Example: Create an index for the `expire` field:

    await myColl.createIndex( expire, { "expiresAfterSeconds": 16 } ) 

If `expire` has a numeric value, the documents expire after the value seconds.

## Index option "expiresAt"

Example: Create an index for the `date` field:

    await myColl.createIndex( date, { "expiresAt": 16 } ) 

The `date` field defines the expiry date. The value can be 
- epoch time value (ms since 1970)
- Date
- String containing a (parsable) date
