# Don't panic

Regardless if the cluster is in panic or there are some corrupt files:
Everything can be solved.

But: **No Backup ... no cry.** If you don't have backups, perhaps it's a good idea to create one now: *Admin GUI -> Backup -> Execute Backup*

Important: Do you have the latest version and all hot fixes deployed.
Nope? You should!!

For all further steps we assume a K8s deployment (but classic HW or VMs work similar).

## Check logs

Please check the error in the logs and copy the log into a file. 
Sometimes it's hard to access logs of crashed or restarted pods.

Bunyan can render the logs more readable, for this
you need to install: `npm i bunyan`

Assuming the stateful set is deployed in the "db" namespace, 
you can get the logs for pod 0 e.g.:

    kubectl logs -n db option-one-db-0 | node_modules/bunyan/bin/bunyan

or if you want to follow th logs 

    kubectl logs -n db option-one-db-0 -f | node_modules/bunyan/bin/bunyan

There should not be much output in the console logs in "info" level after cluster initialization. Only important activities are logged, e.g. database creation or re-indexing.

Remark: Warning and error logs are stored in the "admin/log" database for some time. Please ensure that your pods are not screaming for help all the time., because this will eat up storage!

## Repair corrupt files.

Normally the system is self-healing, e.g. corrupt indexes are repaired automatically or missing files are created automatically. 
Also shut-downs or rolling updated do a graceful stop.

If there is something corrupt for whatever reason:
Basically it is all file based. Files are human readable, it's all JSON and simplified in a cluster the RabbitMQ replicate files between the pods.

To repair files on a pod, open a shell to access the DB files:

    kubectl exec -it -n db option-one-db-0 -- sh

By default the DB files are in the folder `/db` under the pod id:

    cd /db/option-one-db-09000option-one-db/

(Backups are in the `/backup` folder by default.)

To stop the pod create a "stop" file, wait some seconds check the logs that the pod has stopped and delete "stop" file again (important).

    touch stop
    ...
    rm stop

The pod is still alive and partially functional. 
But since it's stopped you should be able to repair files w/o any conflicts.

In the main folder are two important files:
* node.json
* token.json

It is important to check the integrity of this files. 
In this files is everything the pod needs to know about himself and other pods in the cluster.

* Every database has a folder and every collection has a subfolder, each with JSON files containing the configuration.
* The documents are in the collections "doc" folder and index files are in the "idx" folder. The documents are distributed into the token subfolder-hierarchy.
* One important database you don't see directly in the admin GUI is the "admin" database. Please ensure all documents in the "admin" db have valid JSON format.

As soon as all files are intact again, it's time to restart the pod. Logout and kill it:

    exit
    kubectl delete pod -n db option-one-db-0 

Watch the logs to check everything is all right again.