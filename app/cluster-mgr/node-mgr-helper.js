const log = require( '../helper/logger' ).log

module.exports = {
  init,
  distributeTokensToNodes,

  tokenMap,

  genTokens,
  getMaxTokenCnt,
  getNodeNamesSorted,
  pickRandomNode,
  printNodes,
  printNodesShort
}

let clusterNodes = {}

let OWN_NODE_ADDR = null
let TKN_LEN = 1
let DATA_REPLICATION = 3

function init( nodes, ownAddr, tokenLen, replication ) {
  log.debug( 'Init node mgr helper', nodes, ownAddr, tokenLen, replication )
  clusterNodes = nodes
  OWN_NODE_ADDR = ownAddr
  TKN_LEN = tokenLen
  DATA_REPLICATION = replication
}

function tokenMap() {
  let tokenMap = {}
  for ( const node in clusterNodes ) {
    for ( const token in clusterNodes[ node ].token ) {
      tokenMap[ token ] = node
    }
  }
  return tokenMap
}


function distributeTokensToNodes( ) {
  let nodes = getNodeNamesSorted()
  let tokeArr = genTokens()
  let nCnt = 0
  for ( let node in clusterNodes ) {
    clusterNodes[ node ].nodeId = nCnt + ''
    nCnt++
  }
  let i = 0
  for ( let token of tokeArr ) {
    let selNode = nodes[ i % nodes.length ]

    let replNIds = {}
    for ( let cnt = 0; cnt < DATA_REPLICATION; cnt++ ) {
      replNIds[ ( i + cnt ) % nodes.length ] = { status: ( cnt == 0 ? 'master' : 'replica' ) }
    }
    clusterNodes[ selNode ].token[ token ] = {
      sz : 0,    // storage [GB]
      ld : 0,    // load [qry/h]
      an : null, // assist node, perhaps data is there?
      replNodeId : replNIds
    }
    clusterNodes[ selNode ].status = 'Init Tokens'
    i++
  }
}

/* tokens are hex *lowercase of token length  */
function genTokens( ) {
  let len = parseInt( TKN_LEN ,10 ) 
  if ( len == NaN ) { len = 2 }
  let maxHex = getMaxTokenCnt( len )
  let tokens = []
  log.info( 'Gen Tokens: len=',len, 'maxHex=', maxHex )
  for ( let i = 0; i < maxHex; i++ ) {
    let hex = Number( i ).toString( 16 )
    while ( hex.length < len ) { hex = '0' + hex }
    tokens.push( hex )
  }
  return tokens
}


// ===========================================================================

function getNodeNamesSorted() {
  let nodeArr = []
  for ( let node in clusterNodes ) {
    nodeArr.push( node )
  }
  nodeArr.sort()
  return nodeArr
}


// ===========================================================================

function pickRandomNode() {
  let clusterSize = 0
  for ( let node in clusterNodes ) { clusterSize++ }

  let randomPos = Math.floor( Math.random() * ( clusterSize - 1 ) ) + 1
  if ( clusterSize == 1 ) { randomPos = 0 } // should never get here
  
  let pos = 0
  for ( let node in clusterNodes ) {
    if ( pos === randomPos ) {
      log.debug( 'randomNode', clusterSize, node )
      return node
    }
    pos ++  
  }
}

// ===========================================================================

function printNodes( txt ) {
  let nodeStr = txt + '\n'
  for ( let nodeName in clusterNodes ) {
    let no = clusterNodes[ nodeName ]
    nodeStr += nodeName + '   <<' + no.status + '>>\n'
    for ( let tn in no.token ) {
      let t = no.token[ tn ]
      nodeStr += ( t.handover ? '   *' : '    ' )  + tn + ': '
      nodeStr += t.sz + ' GB  ' + t.ld + ' qry/min  ' 
      nodeStr += ( t.an ? '  assist: '+t.an : '' ) + '\n'
    }
  }
  log.info( nodeStr )
}

function printNodesShort( txt ) {
  let nodeStr = txt + '\n'
  for ( let nodeName in clusterNodes ) {
    let no = clusterNodes[ nodeName ]
    nodeStr += nodeName + '   <<' + no.status + '>>  [ '
    for ( let tn in no.token ) {
      let t = no.token[ tn ]
      if ( t.handover === true ) {
        nodeStr += tn + '* '
      } else  if ( t.handover  ) {
        nodeStr += tn + '['+t.handover+'] '
      } else {
        nodeStr += tn + ' '
      }
    }
    nodeStr += ']\n'
  }
  log.info( nodeStr )
}


function getMaxTokenCnt( tokenLen ) {
  let len = TKN_LEN
  let maxHex = 256 
  if ( len == 1 ) { maxHex =    16 } else
  if ( len == 2 ) { maxHex =   256 } else
  if ( len == 3 ) { maxHex =  4095 } else 
  if ( len == 4 ) { maxHex = 65535 } 
  else { 
    maxHex = 256
  }
  return maxHex
}