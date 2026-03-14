# DB Cluster Set Up

## RabbitMQ (prerequisite)

An asynchronous pub/sub mechanism is used to sync nods and replicate data. 

Start a local RabbitMQ:

    docker run -d --hostname rabbitmq --name rabbitmq -e RABBITMQ_DEFAULT_USER=adminuser -e RABBITMQ_DEFAULT_PASS=adminpassword  -p 8080:15672  -p 4369:4369  -p 5672:5672 -p 15672:15672 rabbitmq:4-management

Login to RabbitMQ Web GUI: http://localhost:15672

It's recommended to create (alt least one) dedicated user for the DB cluster nodes.

    export RMQ_URL="amqp://USER:PWD@RABBITMQ_HOST"
    export RMQ_PREFIX: "DB_"

## Start Cluster ... with default config

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
export VERSION="0.8"
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

## Admin GUI

Admin GUI is (e.g.) http://localhost:9000/db/

## Data Directory

Default is `./db/` but of course you can change it

    export DATA_DIR=some_path

Every node will create its own subfolder. 

## Adding a Data Node

Start a new pod and configure it for your cluster, e.g.:

    export NODE_ENV=all-local3
    export DB_CLUSTER_NAME=some-name
    export DB_CLUSTER_KEY=some-key
    export DB_SEED_PODS=some-db:9000/db
    node app | node_modules/bunyan/bin/bunyan

Open the Admin GUI in your Browser and click on the "ADD" in the new node.

The tokens will be [[admin-replica.mn|re-distributed]], this may take some time. 

Please be patient and wait for the new node to get to the "OK" status, before adding another node.


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
It's recommended to scale one pod after another and wait until the new member is populated with data before adding another pod.

The cluster is self organizing.
The data is re-distributed to new cluster members by a high efficient algorithm.
This minimizes data transfers, but still distributes the load evenly.

## Define API URL Path (default: /db)

Example:

    export DB_API_PATH=/mydb

By default the URL path is `/option-one-db`.

## Configure DB vs API/GUI pods (TODO)

You can separate the DB from API pods, by `DB_NODE_TYPE` configuration:

    export DB_NODE_TYPE=DB

vs

    export DB_NODE_TYPE=API

## Configure Seed Pods

If your StateFulSet is "db", configure this for all pods:

    export DB_SEED_PODS="db-0:9000/option-one-db;db-1:9000/db;db-2:9000/option-one-db"

If you have changed the API URL Path, you have to replace the `option-one-db` respectively.

## Configure Security

DB password rule can be configured by environment variables.

Default is:

    export DB_PASSWORD_REGEX="^(?=.*[A-Z].*)(?=.*[!@#$&*+\]\}\[\{\-_=].*)(?=.*[0-9].*)(?=.*[a-z].).{12,}$"

Means:
- must contain at least one upper case characters
- must contain at least one special character 
- must contain at least one number
- must contain at least one lower case characters
- must have a minimum length of 12

You can add some hint for GUI and errors

    export DB_PASSWORD_REGEX_HINT="Description for the password rules ..."


## Start the Cluster

Just start 

It's recommended to start the "seed pod" first.


# Configuration Parameter Reference

The config parameters can be passed 
1. in the `initDB( params )` as properties of the `params` object or
2. as environment variables (has priority)
<table>
<thead>
<tr><th style="text-align:left">Configuration Param</th><th style="text-align:left">Explanation</th><th style="text-align:left">Default Value</th></tr>
</thead>
<tbody>
<tr><td style="text-align:left">ADMIN_PWD</td><td style="text-align:left">"admin" password</td><td style="text-align:left"><em><code>undefined</code></em></td></tr>
<tr><td style="text-align:left">API_PATH</td><td style="text-align:left">Path for GUI and API URL</td><td style="text-align:left"><code>"/db"</code></td></tr>
<tr><td style="text-align:left">API<em>PARSER</em>LIMIT</td><td style="text-align:left">API limit for POST body size</td><td style="text-align:left"><code>"10mb"</code></td></tr>
<tr><td style="text-align:left">APP_NAME</td><td style="text-align:left">Title in admin GUI</td><td style="text-align:left"><code>"Option-One DB"</code></td></tr>
<tr><td style="text-align:left">BACKUP_DIR</td><td style="text-align:left">Root directory for backup files</td><td style="text-align:left"><code>"./backup/"   </code></td></tr>
<tr><td style="text-align:left">DATA_REPLICATION</td><td style="text-align:left">Cluster: Data replication</td><td style="text-align:left"><code>3</code></td></tr>
<tr><td style="text-align:left">DATA_REGION</td><td style="text-align:left">unused yet</td><td style="text-align:left"><code>"EU"</code></td></tr>
<tr><td style="text-align:left">DATA_DIR</td><td style="text-align:left">Root directory for data files</td><td style="text-align:left"><code>"./db/"</code></td></tr>
<tr><td style="text-align:left">DB<em>PASSWORD</em>REGEX</td><td style="text-align:left">Password rule</td><td style="text-align:left"><code>"^(?=.<em>[A-Z].</em>)(?=.<em>[!@#$&</em>+]}[{-_=].<em>)(?=.</em>[0-9].<em>)(?=.</em>[a-z].).{8,}$</code>"`</td></tr>
<tr><td style="text-align:left">DB<em>PASSWORD</em>REGEX_HINT</td><td style="text-align:left">Hint in GUI for password change</td><td style="text-align:left"><code>"Password minimum length must be 8, must contain upper and lower case letters, numbers and extra characters !@#$&*+-_=[]{}"</code></td></tr>
<tr><td style="text-align:left">DB<em>POD</em>NAME</td><td style="text-align:left">If you need to override <code>$HOSTNAME</code></td><td style="text-align:left"><code>$HOSTNAME</code></td></tr>
<tr><td style="text-align:left">DB<em>SEED</em>PODS</td><td style="text-align:left">URL of node which should take the lead for cluster operations<br>(e.g. <code>localhost:9000/db</code>)</td><td style="text-align:left"><em><code>undefined</code></em></td></tr>
<tr><td style="text-align:left">ERR<em>LOG</em>EXPIRE_DAYS</td><td style="text-align:left">Retention for error logs (days)</td><td style="text-align:left"><code>31</code></td></tr>
<tr><td style="text-align:left">GUI<em>SHOW</em>CLUSTER</td><td style="text-align:left">Show cluster tab in admin GUI</td><td style="text-align:left"><code>true</code></td></tr>
<tr><td style="text-align:left">GUI<em>SHOW</em>ADD_DB</td><td style="text-align:left">Show "Add DB" form in admin GUI</td><td style="text-align:left"><code>true</code></td></tr>
<tr><td style="text-align:left">GUI<em>SHOW</em>USER_MGMT</td><td style="text-align:left">Show user management in GUI</td><td style="text-align:left"><code>true</code></td></tr>
<tr><td style="text-align:left">MAX<em>ID</em>SCAN</td><td style="text-align:left">Max docs in a full scan query</td><td style="text-align:left"><code>10000</code></td></tr>
<tr><td style="text-align:left">MAX<em>CACHE</em>MB</td><td style="text-align:left">Size of in-memory-cache (MB)</td><td style="text-align:left"><code>10</code></td></tr>
<tr><td style="text-align:left">MODE</td><td style="text-align:left"><code>"RMQ"</code> for multi node cluster, <code>"SINGLE_NODE"</code> for a one node DB</td><td style="text-align:left"><code>"RMQ"</code></td></tr>
<tr><td style="text-align:left">NODE<em>SYNC</em>INTERVAL_MS</td><td style="text-align:left">Cluster: The sync interval of the nodes (ms)</td><td style="text-align:left"><code>10000</code></td></tr>
<tr><td style="text-align:left">PORT</td><td style="text-align:left">Port for GUI and API</td><td style="text-align:left"><code>9000</code></td></tr>
<tr><td style="text-align:left">RMQ_URL</td><td style="text-align:left">RabbitMQ URL for multi-node</td><td style="text-align:left"><code>"amqp://localhost"</code></td></tr>
<tr><td style="text-align:left">RMQ_PREFIX</td><td style="text-align:left">RabbitMQ queue name prefix</td><td style="text-align:left"><code>"DB_"</code></td></tr>
<tr><td style="text-align:left">RMQ<em>JOB</em>EXCHANGE</td><td style="text-align:left">RabbitMQ job topic name</td><td style="text-align:left"><code>"DB<em>node</em>jobs"</code></td></tr>
</tbody>
</table>


[[main.md|Back to index]]