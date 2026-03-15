# Option-One-DB 

Option One DB is the next generation open source document database:
- Fast and light weight
- Scales horizontally
  ... but runs as single server on a laptop or even a Raspberry Pi
- Optimized to run in a container and Kubernetes
- Powerful indexing and query engine
- Integrated GUI for administration, monitoring and data access
- Simple user and API access management
- Built in backup scheduler

![DB admin](screen-db-dark.png) 

Status: EXPERIMENTAL -- use at your own risk!!

## Start a single server DB

Run the server as docker container locally

```bash
docker run -d --name "option_one_db"  -p 9000:9000  -e DB_POD_NAME='my-db' -v /home/my-user/db/:/option-one/db/ -v /home/my-user/backup:/option-one/backup/ mahade70/option-one-db:0.8-single
```

(This creates the folder `/home/my-user/db` and `/home/my-user/backup` if they are not existing.)

Get the user and password from the startup logs:

    docker logs option_one_db

Open http://localhost:9000/db and log in.

Check out the GitLab repo how to run the server as NodeJS process without docker.

## Start a DB cluster in Kubernetes

*Remark: Please understand this deployment as a starting point which you need to improve and harden. I.e. this example does not contain secrets (just "env"), PDBs, network policies, resources, security policies, ...*

Set up a RabbitMQ for the pod-to-pod communication: See https://www.rabbitmq.com/kubernetes/operator/quickstart-operator, login to the admin GUI and create a user and grant access to  `/` virtual hosts.

```bash
RMQ_USER="rabbitmq_username"
RMQ_PWD="rabbitmq_password"
RMQ_NAMESPACE="rabbritmq_namespace"
export RMQ_URL="amqp://${RMQ_USER}:${RMQ_PWD}@rabbitmq.${RMQ_NAMESPACE}"
```
This will also deploy a RabbitMQ and three database pods. 
You can scale the cluster any time later. 

```bash
kubectl create namespace db
export REGISTRY="mahade70"
export ADMIN_PWD="super-secret-password"
export MIN_READY_SECS=5  # for a rolling updates this should be higher, e.g. 60
export VERSION="0.9"
wget https://raw.githubusercontent.com/ma-ha/option-one-db/master/k8s-deploy/option-one-db-3node-cluster.yml
cat option-one-db-3node-cluster.yml | envsubst | kubectl apply -n db -f -
```
The initial admin password is in the logs:

    kubectl logs -n db option-one-db-0 -f

Open http://${K8S-GATEWAY-IP}/option-one-db and log in.

If all cluster nodes are IN "OK" state, the tokens 0..F should be distributed evenly w/o duplicates. 
Logs or cluster GUI should show something like this:

    db01:9011/db   (OK)  [ 0 3 6 9 c f ]
    db02:9012/db   (OK)  [ 1 4 7 a d ]
    db03:9013/db   (OK)  [ 2 5 8 b e ]


## JS SDK usage example

This [JS SDK npm package](https://www.npmjs.com/package/option-one-db) is a wrapper for the Option One DB [REST API](https://github.com/ma-ha/option-one-db/blob/master/app/gui/docu/db-swagger.yml).  

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

Check out the [API Reference](https://github.com/ma-ha/option-one-db-js-sdk/blob/master/README-SDK.md).


## Collection Indexing Modes

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

## Configure a Single Node DB

A single node DB runs the same code. 
Only difference: No RabbitMQ is called, because it don't need to talk to anyone. 

Settings required:

    MODE="SINGLE_NODE"
    DATA_REPLICATION=1
  
Important: Currently it is not supported to extend a single node db to a cluster.

## Min and Max Cluster Size

You need 3 pods to start a cluster. 

The data is split into shards. Shards are identified by a one digit hexadecimal token (the first digit of the document id). So the data is split into 16 shards. 
Every data chard is replicated multiple times -- by default every shard is stored on 3 pods. So having 3 pods, every pod stores all shards.

Adding more pods has several advantages:
1. The load of data operations can be distributed to more hardware.
2. Huge databases can be optimized, because each pod need to handle less data, which can speed up i.e. complex queries.
3. Data recovery of a total failed pod or a restoring a backup will be faster.

You can scale the cluster until you have 16 master nodes and 2x16 replica only nodes. So the the maximum cluster size is 48 data nodes (pods).

## Configure Replication

By default data is stored in 3 replicas: One master and 2 slaves. 
The replicas are always in different pods. The default quorum is 2, means:
If 2 replica pods say OK, the DB transaction is committed. 

Resulting in these DB modes:

1. `DATA_REPLICATION=3` (default for cluster)
   ... requires min 3 DB pods initially. More are welcome, but can be added any time. Will continue to work if one pod is temporarily not available.
2. `DATA_REPLICATION=2` (not recommended)
   requires 2 DB pods, to run some master/master mode.
3. `DATA_REPLICATION=1`
   for single server DB.

Currently it's not supported to change the `DATA_REPLICATION` for a existing DB. 

## Adding Pods To A Cluster

You can just raise the replica count. 
New pods show up in the GUI and you must click the "Add" button to join them to the cluster.
It's recommended to wait until the new node is populated witch data before adding another node.

The cluster is self organizing.
The data is re-distributed to new cluster members by a high efficient algorithm.
This minimizes data transfers, but still distributes the load evenly.

# License 

See [Option One DB License](LICENSE.md) 

The plan is to release public docker container images quarterly.

If you need 
- direct priority support
- **security updates** and **bug fixes** as soon as they are available
- a version with **special features**
 - you plan to offer the Option One DB as a **hosted or managed service**?

Don't hesitate to contact me: admin at mh-svr.de
