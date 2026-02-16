# Option-One-DB 

Option One DB is the next generation open source document database:
- Fast and light weight
- Scales horizontally
  ... but runs as single server on a laptop or even a Raspberry Pi
- Optimized to run in a container and Kubernetes
- Powerful [indexing and query](README-query.md) engine
- Integrated GUI for administration, monitoring and data access
- Simple user and API access management
- Built in backup scheduler

![DB admin](screen-db-dark.png) 

Status: EXPERIMENTAL -- use at your own risk!!

## Start a single server DB

Run the server as docker container locally

    docker run -d --name option_one_db  -p 9000:9000  -e DB_POD_NAME='my-db' -v /home/my-user/db/:/option-one/db/ -v /home/my-user/backup:/option-one/backup/ mahade70/option-one-db:0.8-single

(This creates the folder `/home/my-user/db` and `/home/my-user/backup` if they are not existing.)

Get the user and password from the startup logs:

    docker logs option_one_db

Open http://localhost:9000/db and log in.

Check out the GitLab repo how to run the server as NodeJS process without docker.

## Start a DB cluster in Kubernetes

*Remark: Please understand this deployment as a starting point which you need to improve and harden. I.e. this example does not contain secrets (just "env"), PDBs, network policies, resources, security policies, ...*

Set up a RabbitMQ for the pod-to-pod communication: See https://www.rabbitmq.com/kubernetes/operator/quickstart-operator, login to the admin GUI and create a user and grant access to  `/` virtual hosts.

    RMQ_USER=rabbitmq_username
    RMQ_PWD=rabbitmq_password
    RMQ_NAMESPACE=parbbritmq_namespace
    export RMQ_URL="amqp://${RMQ_USER}:${RMQ_PWD}@rabbitmq.${RMQ_NAMESPACE}"

This will also deploy a RabbitMQ and three database pods. 
You can scale the cluster any time later. 

    kubectl create namespace db
    export REGISTRY="mahade70"
    export ADMIN_PWD="super-secret-password"
    export MIN_READY_SECS=5  # for a rolling updates this should be higher, e.g. 60
    export VERSION="0.8"
    wget https://raw.githubusercontent.com/ma-ha/option-one-db/master/k8s-deploy/option-one-db-3node-cluster.yml
    cat option-one-db-3node-cluster.yml | envsubst | kubectl apply -n db -f -

The initial admin password is in the logs:

    kubectl logs -n db option-one-db-0 -f

Open http://${K8S-GATEWAY-IP}/option-one-db and log in.

## JS SDK usage example

See https://github.com/ma-ha/option-one-db-js-sdk

```JS 
const { DbClient } = require( 'option-one-db' )
const dbCredentials = { 
  accessId: process.env.DB_ACCESS_ID
  accessKey: process.env.DB_ACCESS_KEY
} 
const client = new DbClient( process.env.DB_URL, dbCredentials )
await client.connect()
const db = await client.db( 'test-db' )
let myAwesomeDocs = await db.collection( 'my-awesome-docs' )
let cursor = myAwesomeDocs.find({ name: 'Moe' })
let docArray = await cursor.toArray()
for ( let doc of docArray ) {
  console.log( doc )
}
```

## Document Collection Modes

Option-One DB supports 2 collection modes:

1. Insert any JSON document: `_id` is a random hex number
2. Insert doc with a primary key (PK): `_id` is the hash of PK fields, where PK is an array of field names.

In both modes you can find documents by `_id` and any indexed field -- or any un-indexed field, but slower.

In collection of type 1 you can insert the same document multiple times.

In collection type 2 you get an error, if you try to insert a doc, where an existing doc has the same PK. Insert will also fail s all PK fields are missing in the document. 

## Define API and GUI URL Path (default: /db)

By default the URL path is `/db` so admin GUI is e.g. `http://localhost:9000/db`

Example:

    export DB_API_PATH=/some-path

will result in GUI/API URL: `http://localhost:9000/some-path`

# Configuration Parameter Reference

The config parameters can be passed 
1. in the `initDB( params )` as properties of the `parms` object or
2. as environment variables (has priority)

| Parameter              | Explanation                     | Default Value        |
|------------------------|---------------------------------|----------------------|
| ADMIN_PWD              | "admin" password                | *`undefined`*        |
| API_PATH               | Path for GUI and API URL        | `"/db"`              |
| API_PARSER_LIMIT       | API limit for POST body size    | `"10mb"`             |
| APP_NAME               | Title in admin GUI              | `"Option-One DB"`    |
| BACKUP_DIR             | Root directory for backup files | `"./backup/"   `     |
| DATA_REPLICATION       | Cluster: Data replication       | `3`                  |
| DATA_REGION            | unused yet                      | `"EU"`               |
| DATA_DIR               | Root directory for data files   | `"./db/"`            |
| DB_PASSWORD_REGEX      | Password rule                   | `"^(?=.*[A-Z].*)(?=.*[!@#$&*+]}[{-_=].*)(?=.*[0-9].*)(?=.*[a-z].).{8,}$`"` |
| DB_PASSWORD_REGEX_HINT | Hint in GUI for password change | `"Password minimum length must be 8, must contain upper and lower case letters, numbers and extra characters !@#$&*+-_=[]{}"` |
| DB_POD_NAME            | If you need to override `$HOSTNAME` | `$HOSTNAME`      |
| DB_SEED_PODS           | URL of node which should take the lead for cluster operations (e.g. `localhost:9000/db`) |  *`undefined`* |
| ERR_LOG_EXPIRE_DAYS    | Retention for error logs (days) | `31`                 |
| GUI_SHOW_CLUSTER       | Show cluster tab in admin GUI   | `true`               |
| GUI_SHOW_ADD_DB        | Show "Add DB" form in admin GUI | `true`               |
| GUI_SHOW_USER_MGMT     | Show user management in GUI     | `true`               |
| MAX_ID_SCAN            | Max docs in a full scan query   | `10000`              |
| MAX_CACHE_MB           | Size of in-memory-cache (MB)    |  `10`                |
| MODE                   | `"RMQ"` for multi node cluster, `"SINGLE_NODE"` for a one node DB |  `"RMQ"` |
| NODE_SYNC_INTERVAL_MS  | Cluster: The sync interval of the nodes (ms) |  `10000` |
| PORT                   | Port for GUI and API            | `9000`               |
| RMQ_URL                | RabbitMQ URL for multi-node     | `"amqp://localhost"` |
| RMQ_PREFIX             | RabbitMQ queue name prefix      | `"DB_"`              |
| RMQ_JOB_EXCHANGE       | RabbitMQ job topic name         | `"DB_node_jobs"`     |
| TOKEN_LEN              | Tokens are hex and the max number defines the max nodes in the cluster, TOKEN_LEN=1 max 16 nodes  |  `1` |

## Configure a Single Node DB

A single node DB runs the same code. 
Only difference: No RabbitMQ is called, because it don't need to talk to anyone. 

Settings required:

    MODE="SINGLE_NODE"
    DATA_REPLICATION=1
  
Important: Currently it is not supported to extend a single node db to a cluster.

## Max Cluster Size: Default is 16 Pods

The maximum cluster pod count is defined by the TOKEN_LEN:
- export TOKEN_LEN=1 
  to set the max. DB pods count to 16 (= default)
- export TOKEN_LEN=2
  to set the max. DB pods count to 256
- export TOKEN_LEN=3 
  to set the max. DB pods count to 4095
- export TOKEN_LEN=4 
  to set the max. DB pods count to 65535

A larger TOKEN_LEN also comes with more internal overhead - 
so don't set the TOKEN_LEN to 2 or 3 or 4 without any reason!

## Configure Replication

By default data is stored in 3 replicas, which are always in different pods. The default quorum is 2, means:
If 2 replica pods say OK, the DB transaction is committed. 

Resulting in these DB modes:

1. `DATA_REPLICATION=3` (default)
   ... requires min 3 DB pods initially. More are welcome, but can be added any time. Will continue to work if one pod is temporarily not available.
2. `DATA_REPLICATION=2` (not recommended)
   requires 2 DB pods, to run some master/master mode.
3. `DATA_REPLICATION=1`
   for single server DB.

Currently it's not implemented to change the `DATA_REPLICATION` for a existing DB. 

# License 

See [Option One DB License](LICENSE.md) 

The plan is to release public docker container images quarterly.

If you need 
- direct priority support
- **security updates** and **bug fixes** as soon as they are available
- a version with **special features**
 - you plan to offer the Option One DB as a **hosted or managed service**?

Do not hesitate to contact me: admin at mh-svr.de
