const optionOneDB = require( './app' )

optionOneDB.startDB({
  DATA_DIR         : "./db/",
  BACKUP_DIR       : "./backup/",
  DATA_REPLICATION : 1,
  ENV              : "single-node",
  MODE             : "SINGLE_NODE",
  LOG_LEVEL        : "info"
})