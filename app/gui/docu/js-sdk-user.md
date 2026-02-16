# Class "DbClient"

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

## async addUser( userId, password, email, database, [admin = false] )

Create and authorize a user. Need admin rights.

Parameter:
* `userId`: String, allowed: [a..z,-,0..9]
* `password`: String
* `email`: String
* `database`: String or Array of DB names, where the user should be authorized. Use "*" to grant to all DBs.
* `admin`: boolean

Returns: `{ ok: true }` or `{ error: "text" }`

## async changeUser( userId, password, email, database, [admin = false] )

Change a user.  Need admin rights.

##  async deleteUser( userId )

Delete a user  Need admin rights.

[[main.md|Back to index]]