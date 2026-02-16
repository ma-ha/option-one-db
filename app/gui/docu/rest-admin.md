# Admin REST API Reference


## POST /cluster/node

Add new database node to cluster.

*Remark: New node must be running and initialized into the "NEW" state, see logs.*

Query parameters:
- pod: Endpoint of new node, e.g. "mh-UX305CA:9003/db"

## POST /db

Create a database.

Body:
- name: DB name 

Response:
- 202 Accepted
- 400 Bad request
- 500 Server error
  
Authorization: Admin role
  
## DELETE /db/:db

Drop a database.

Parameters:
- db: DB name 

Authorization: Admin role

## POST /db/backup

Create full backup.

Authorization: Admin role

## POST /db/:db/backup

Create backup of one database.

Parameters:
- db: DB name 

Authorization: Admin role

## POST /db/:db/:coll/backup

Create backup of one collection.

Parameters:
- db: DB name 
- coll: Collection name 

Authorization: Admin role

