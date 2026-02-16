module.exports = {
  setConfig,
  getAllConfigs
}

let allConfigs = {}

function setConfig( config, params ) {
  for ( let key in config ) {
    allConfigs[ key ] = config[ key ]
    if ( process.env[ key ] ) {
      config[ key ] = process.env[ key ]
    } else  if ( params && params[ key ] ) {
      config[ key ] = params[ key ]
    }
  }
}

function getAllConfigs() {
  return allConfigs
}