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

You can just deploy the StatefulSet with three pods into a Kubernetes and trust the defaults.

TL;DR:

    export NODE_ENV=all-local
    export DB_CLUSTER_NAME=my-cluster
    export DB_CLUSTER_KEY=secret
    # start 1st db-node
    export DB_PORT=9000
    node app &
    export DB_SEED_PODS=localhost:9000/db
    # start 2nd db-node
    export DB_PORT=9001
    node app &
    # start 3rd db-node
    export DB_PORT=9002
    node app &

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


## Configure Max Cluster Size (default: 16 Pods)

The maximum cluster pod count is defined by the TOKEN_LEN:
- `export TOKEN_LEN=1` 
  to set the max. DB pods count to 16 (= default)
- `export TOKEN_LEN=2`
  to set the max. DB pods count to 256
- `export TOKEN_LEN=3` 
  to set the max. DB pods count to 4095
-` export TOKEN_LEN=4` 
  to set the max. DB pods count to 65535

A larger TOKEN_LEN also comes with more internal overhead - 
so don't set the TOKEN_LEN to 2 or 3 or 4 without any reason!

## Configure Replication

By default data is stored in 3 replicas, which are always in different pods.

The default quorum is 2, means:
If 2 replica pods say OK, the DB transaction is committed. 

You can reduce DATA_REPLICATION to 1, resulting in these DB modes:

1. `DATA_REPLICATION=3` (default)
   ... requires min 3 DB pods initially. More are welcome, but can be added any time. Will continue to work if one pod is temporarily not available.
2. `DATA_REPLICATION=2` (not recommended)
   requires 2 DB pods, to run some master/master mode.
3. `DATA_REPLICATION=1`
   Ideal for local development with only one DB pod and without any redundancy and load distribution.

Currently it's not implemented to change the `DATA_REPLICATION` for a existing DB. 

Technical precondition is, that all pods can communicate via HTTP(S) to each other.

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
<tr><td style="text-align:left">TOKEN_LEN</td><td style="text-align:left">Tokens are hex and the max number defines he max nodes in<br> the cluster, TOKEN_LEN=1 max 16 nodes</td><td style="text-align:left"><code>1</code></td></tr>
</tbody>
</table>


[[main.md|Back to index]]