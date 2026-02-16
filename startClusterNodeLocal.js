const optionOneDB = require( './app' )

optionOneDB.startDB({
  DATA_DIR         : "./db/",
  BACKUP_DIR       : "./backup/",
  ENV              : "cluster-node",
  LOG_LEVEL        : "info"
})