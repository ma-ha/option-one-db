const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const helper   = require( './node-mgr-helper' )
const fs       = require( 'fs' )
const { mkdir, open, writeFile, readFile } = require( 'node:fs/promises' )

module.exports = {
  init,
  getHelper,
  checkStopReq,
  initSeedNodes,

  getSyncData,
  setNodeIdMap,
  mergeNodes,

  ownNodeId,
  getOwnNodeStatus,
  getOwnStatus,
  getOwnTokens,
  writeNodeInfo,
  getClusterNodes,
  getClusterNode,
  getClusterSize,
  getTokens,
  getTokensForNodeId,
  getOwnMasterAndReplicaTokens,
  addTokenReplica,
  isFirstClusterNode,

  setStatus,
  getStatusSince,
  isSyncing,
  allNodesInSyncingStat,
  allNodesOK,

  handleNodeUnavailable,
  setNodeAvailable,

  setOnboardingStatus,
  updTokenStatus,
  getAnnouncedToken,
  // lookupCandidateForTokenHandover,
  // askForTokenHandover,
  // handoverToken,
  // handoverTokenStatus,

  pushTaskToQueue
}

// ===========================================================================

let dtaDir = null 

let clusterNodes = {}

let myNodeStatus = null

const syncTokenStatus = {}

// ----------------------------------------------------------------------------

let cfg = {
  OWN_NODE_ADDR : 'localhost:9000/db',
  DATA_REPLICATION: 3,
  DATA_REGION: 'EU',
  DATA_DIR : './db/',
  DB_SEED_PODS : null,
  TOKEN_LEN : 1
}

async function init( configParams ) {
  log.info('Init node mgr helper...' )
  cfgHlp.setConfig( cfg, configParams )

  dtaDir = cfg.DATA_DIR
  if ( ! dtaDir.endsWith('/') ) { dtaDir += '/'  }
  dtaDir += cfg.OWN_NODE_ADDR.replaceAll(':','').replaceAll('/','') + '/'
  if ( ! fs.existsSync( dtaDir ) ) {
    await mkdir( dtaDir, { recursive: true } )
  }
  log.info( 'DB dir:', dtaDir )

  await readNodeInfo()
  clusterNodes[ cfg.OWN_NODE_ADDR ] = myNodeStatus
  helper.init( clusterNodes, cfg.OWN_NODE_ADDR, cfg.TOKEN_LEN, cfg.DATA_REPLICATION )
  return helper
}

function getHelper() {
  return helper
}


function checkStopReq() {
  if ( fs.existsSync( dtaDir+'/stop' ) ) {
    setStatus( 'Stopped' )
    return true
  }
  return false
}

// ============================================================================

let oldNodeInf = ''
async function writeNodeInfo() {
  let newNodeInf =  getNodeInfStr()
  if ( oldNodeInf != newNodeInf ) {
    log.info( 'writeNodeInfo', newNodeInf )
    await writeFile( dtaDir + '/node.json', JSON.stringify( myNodeStatus, null, '  ' ) )
    await writeFile( dtaDir + '/token.json', JSON.stringify( myNodeStatus.token, null, '  ' ) )
    oldNodeInf = newNodeInf
  }
} 

function getNodeInfStr() {
  let nodeInfStr = myNodeStatus.status + ': '
  for ( let node in myNodeStatus.nodeIdMap ) {
    nodeInfStr += myNodeStatus.nodeIdMap[ node ]
  }
  for ( let tk in myNodeStatus.token ) try {
    nodeInfStr += ' ' +tk +'='
    for ( let no in myNodeStatus.token[ tk ].replNodeId ) {
      nodeInfStr +=  no + myNodeStatus.token[ tk ].replNodeId[ no ].status?.substr(0,1) 
    }
  } catch( exc ) { log.warn( 'getNodeInfStr', exc )}
  return nodeInfStr
}

async function readNodeInfo( ) {
  if ( fs.existsSync( dtaDir + '/node.json' ) ) {
    log.info('... reading node.json')
    let token = {}
    if ( fs.existsSync( dtaDir + '/token.json' ) ) {
      log.info('... reading token.json')
      token = JSON.parse( await readFile( dtaDir + '/token.json' ) )
    }
    try {
      let status = JSON.parse( await readFile( dtaDir + '/node.json' ) )
      if (  status.upd ) {
        myNodeStatus = {
          nodeId  : status.nodeId,
          podName : cfg.OWN_NODE_ADDR,
          status  : status.status,
          color   : null,
          storage : null,
          token   : token,
          upd     : status.upd,
          nodeIdMap : status.nodeIdMap
        }
        if ( status.nodeId != null ) {
          myNodeStatus.nodeIdMap[ status.nodeId ] = cfg.OWN_NODE_ADDR
        }

        if ( status.pullTokenTODO   ) { myNodeStatus.pullTokenTODO = status.pullTokenTODO }
        if ( status.taskQueue ) { myNodeStatus.taskQueue = status.taskQueue }
        return 
      }
    } catch (error) { log.warn( 'readNodeInfo', error.message ) }
  }
  myNodeStatus = {
    nodeId  : null,
    podName : cfg.OWN_NODE_ADDR,
    status  : 'NEW',
    color   : null,
    storage : null,
    token   : {},
    upd     : Date.now(),
    nodeIdMap : {},
    taskQueue : []
  }
  
} 

async function setOnboardingStatus( newNodeId ) {
  log.info( 'setOnboardingStatus', newNodeId )
  myNodeStatus.nodeId = newNodeId + ''
  // myNodeStatus.pullTokenTODO = tokens
  setStatus( 'Onboarding' )
  await writeNodeInfo()
}


async function pushTaskToQueue( task ) {
  log.debug( 'pushTaskToQueue ', task )
  if ( ! myNodeStatus.taskQueue ) { myNodeStatus.taskQueue = [] }
  myNodeStatus.taskQueue.push( task )
  await writeNodeInfo()
}

// ============================================================================

async function initSeedNodes() { // TODO handle: seed pod responds with different name
  // get seed node config
  // if ( getOwnStatus() == 'Terminating' ) { setStatus( 'NeedSync' ) }
  let seedPodList = []
  if ( cfg.DB_SEED_PODS ) {
    seedPodList = cfg.DB_SEED_PODS.split( ';' )
  }

  // ask seen nodes
  log.info( 'seedPodList', seedPodList )
  for ( let seedAddr of seedPodList ) {
    addClusterNode( seedAddr, { 
      status : 'seed',
      upd    : Date.now()
    } )
  }

  if ( getOwnStatus() != 'Onboarding' &&  getOwnStatus() != 'NEW' ) {
    setStatus( 'Syncing' )
    log.info( 'initSeedNodes >>>> set Syncing' )
  }
}


function addClusterNode( addr, node ) {
  clusterNodes[ addr ] = node
}

const db  = require( '../db-engine/db' )

async function getSyncData() {
  return {
    from      : cfg.OWN_NODE_ADDR,
    nodes     : getClusterNodes(),
    nodeIdMap : myNodeStatus.nodeIdMap,
    db        : await db.getDbTree()
  }
}

// ===========================================================================

function getOwnNodeStatus() {
  return myNodeStatus
}


function ownNodeId() {
  return myNodeStatus?.nodeId +''
}

function getOwnStatus() {
  return myNodeStatus?.status
}
function getClusterNodes() {
  log.debug( 'clusterNodes', clusterNodes )
  return clusterNodes
}

function getClusterNode( node ) {
  log.debug( 'clusterNodes', clusterNodes )
  return clusterNodes[ node ]
}

function isFirstClusterNode() {
  let nodeNamesSorted = helper.getNodeNamesSorted()
  log.info( 'isFirstClusterNode', nodeNamesSorted, cfg.OWN_NODE_ADDR )
  if ( nodeNamesSorted && nodeNamesSorted[0] == cfg.OWN_NODE_ADDR ) { 
    return true
  }
  return false
}

function isSyncing( node ) {
  if ( node ) {
    if ( clusterNodes[ node ].status === 'Syncing' ) {
      return true
    }
  } else { 
    if ( myNodeStatus.status === 'Syncing' ){
      return true
    }
  }
  return false
}

function isOwnStatus( status ) {
  if ( myNodeStatus.status == status ){
    return true
  } else {
    return false
  }
}


function isNodeStatus( node, status ) {
  if ( clusterNodes[ node ].status == status ){
    return true
  } else {
    return false
  }
}

function allNodesInSyncingStat( ) {
  let isAllSync = true
  let nodeCnt = 0
  for ( let node in clusterNodes ) {
    if ( clusterNodes[ node ].status != 'Syncing' ) {
      isAllSync = false
    } else {
      nodeCnt ++
    }
  }
  if ( isAllSync && nodeCnt < cfg.DATA_REPLICATION ) {
    log.warn( 'allNodesInSyncingStat', nodeCnt, 'Not enough nodes for replicaton='+ cfg.DATA_REPLICATION )
    isAllSync = false
  }
  return isAllSync
}

function allNodesOK( ) {
  for ( let node in clusterNodes ) {
    if ( clusterNodes[ node ].status != 'OK' ) {
      return false
    } 
  }
  return true
}
// ===========================================================================

function getTokens() {
  return myNodeStatus.token
}

function getTokensForNodeId( nodeId ) {
  // log.info( 'getTokensForNodeId >>>> ' , nodeId, clusterNodes )
  let nodeName = myNodeStatus.nodeIdMap[ nodeId ]
  // log.info( 'getTokensForNodeId >>>> ' , nodeName, clusterNodes[ nodeName ] )
  let nodeTokenMap = clusterNodes[ nodeName ].token
  // log.info( 'getTokensForNodeId >>>>>>> ' , nodeName, nodeTokenMap )
  let tokenArr = []
  for ( let token in nodeTokenMap ) {
    // log.info( 'getTokensForNodeId >>>>>>>>>> ' ,tokenArr, token )
    tokenArr.push( token )
  }
  // log.info( 'getTokensForNodeId >>>>>>>>>> ' ,tokenArr )
  return tokenArr
}


function getOwnMasterAndReplicaTokens() {
  let tokens = []
  for ( let node in clusterNodes ) {
    let nodeTokenMap = clusterNodes[ node ].token
    for ( let token in nodeTokenMap ) {
      for ( let replicaNode in nodeTokenMap[ token ].replNodeId ) {
        if ( replicaNode == myNodeStatus.nodeId ) {
          if ( ! tokens.includes( token)) { tokens.push( token ) }
        } 
      }
      
    }
  }
  log.debug( 'getOwnMasterAndReplicaTokens', tokens)
  return tokens
}


async function addTokenReplica( replicaTokens, replicaNodeId ) {
  log.info( 'addTokenReplica', replicaTokens, replicaNodeId  )
  for ( let myToken in myNodeStatus.token ) {
    if ( replicaTokens.includes( myToken ) ) {
      myNodeStatus.token[ myToken ].replNodeId[ replicaNodeId ] = { status : "replica" }
    }
  }
  await writeNodeInfo()
}

// ===========================================================================

async function setNodeIdMap( nodeIdMap ) {
  log.debug( 'setNodeIdMap', nodeIdMap )
  for ( let id in nodeIdMap ) {
    if ( ! myNodeStatus.nodeIdMap[ id ] ) {
      myNodeStatus.nodeIdMap[ id ] = nodeIdMap[ id ] // sync
      if ( myNodeStatus.nodeId == null && nodeIdMap[ id ] == cfg.OWN_NODE_ADDR ) {
        myNodeStatus.nodeId = id + '' // got id assigned
      }
    }
  }
  await writeNodeInfo()
}


async function mergeNodes( nodesUpd ) {
  log.debug( 'mergeNodes ...', nodesUpd )
  printNodesShort( 'Cluster: Onboard node ...' )
  if ( ! nodesUpd ) { return  }// TODO better check 
  for ( let node in nodesUpd ) {

    if ( node != cfg.OWN_NODE_ADDR ) { 
      log.debug( 'update node', node )
    
      if ( ! clusterNodes[ node ] ) {  // need to add node

        log.info( 'add node', node )
        clusterNodes[ node ] = nodesUpd[ node ]

      } else {

        if ( isNodeStatus( node, 'NeedSync' ) ) {
          if ( nodesUpd[ node ].status == 'Syncing' ) {
            setNodeStatus( node, 'Syncing' )
          }
        } else {
          setNodeStatus( node, nodesUpd[ node ].status )
          clusterNodes[ node ].token     = nodesUpd[ node ].token 
          clusterNodes[ node ].taskQueue = nodesUpd[ node ].taskQueue 
        }
      }
    
    } else { // getting data about myself

      function areAllNew() {
        let allNew = true
        for ( let node in clusterNodes ) {
          if (clusterNodes[ node ].status == 'Syncing' ) { continue }
          if (clusterNodes[ node ].status != 'NEW' ) {
            allNew = false
          }
        }
        return allNew
      }
      if ( clusterNodes[ cfg.OWN_NODE_ADDR ] ) {
        //log.info( 'cfg.OWN_NODE_ADDR ].status', clusterNodes[ cfg.OWN_NODE_ADDR ].status )
        if ( clusterNodes[ cfg.OWN_NODE_ADDR ].status == 'NEW' ) {
          if ( areAllNew() ) {
            setStatus( 'Syncing' )
          }
        } else if ( clusterNodes[ cfg.OWN_NODE_ADDR ].status == 'Onboarding' ) {
          // don' tell me anything else
        } else if ( clusterNodes[ cfg.OWN_NODE_ADDR ].status == 'OK' ) {
          // don' tell me anything else
        } else if ( nodesUpd[ node ].status == 'Init Tokens' ) {
          setStatus( 'OK' )
          await setAllMyTokens( nodesUpd[ node ].token )
          // clusterNodes[ cfg.OWN_NODE_ADDR ].token = nodesUpd[ node ].token

        } else if ( nodesUpd[ node ].status == 'NeedSync' || nodesUpd[ node ].status == 'Syncing' ) {
        
          let myTokenCnt = 0
          for ( let token in nodesUpd[ node ].token ) {
            myTokenCnt ++
          }
          if ( myTokenCnt > 0  ) {
            setStatus( 'OK' )
            await setAllMyTokens( nodesUpd[ node ].token )
            // clusterNodes[ cfg.OWN_NODE_ADDR ].token = nodesUpd[ node ].token
          } else {
            log.info( 'mergeNodes >>>> set Syncing' )
            setStatus( 'Syncing' )
          }
        }
      }

    }
  }

  for ( let node in clusterNodes ) {
    if ( clusterNodes[ node ].nodeId != null ) {
      let nid = clusterNodes[ node ].nodeId
      if ( ! myNodeStatus.nodeIdMap[ nid ] ) {
        myNodeStatus.nodeIdMap[ nid ] = node
      }
    }
  }
  for ( let tkn in myNodeStatus.token ) {

  }

  if ( myNodeStatus.status == 'NEW' && myNodeStatus.nodeId != null && Object.keys(  myNodeStatus.token ).length > 0 ) {
    log.warn( 'STATUS LOST, RESYNC .....')
    setStatus( 'Syncing' )
  }

  await writeNodeInfo()
  // log.info( 'clusterNodes', JSON.stringify(clusterNodes,null,' ') )
  printNodesShort( 'Cluster: Onboard node ...' )
}

// ===========================================================================

let statusSince = Date.now()

function setStatus( newStatus ) {
  if ( getOwnStatus() == 'Onboarding' ) {
    return // keep in onboarding
  }
  if ( myNodeStatus.status != newStatus ) {
    myNodeStatus.status = newStatus
    myNodeStatus.upd    = Date.now()
    statusSince = myNodeStatus.upd   
  }
}

function getStatusSince() {
  return statusSince
}

function setNodeStatus( node, newStatus ) {
  if ( clusterNodes[ node ] ) {
    clusterNodes[ node ] .status = newStatus
  }
}


// ===========================================================================
let oldNodeStr = ''
function printNodesShort( txt ) {
  let nodeStr = ''
  for ( let nodeName in clusterNodes ) {
    let no = clusterNodes[ nodeName ]
    nodeStr += nodeName + '   (' + no.status + ')  [ '
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
  if ( nodeStr != oldNodeStr ) {
    log.warn( txt + '\n' + nodeStr )
    oldNodeStr = nodeStr
  }
  // log.info( txt + '\n' + nodeStr )
}

// ===========================================================================

let announceToken = null

function updTokenStatus() { // TODOOOOOOOO
  // log.warn( 'updTokenStatus ...', 'TODO')
  // let tokenCnt = 0
  // for ( let t in clusterNodes[ cfg.OWN_NODE_ADDR ].token ) { tokenCnt++ }
  // if ( tokenCnt > 1 ) {
  //   let cnt = 0
  //   for ( let t in clusterNodes[ cfg.OWN_NODE_ADDR ].token ) {
  //     if ( cnt == tokenCnt - 1 ) { 
  //       if ( ! clusterNodes[ cfg.OWN_NODE_ADDR ].token[ t ].handover ) {
  //         // advertise one token as handover candidate
  //         log.info( '###### announce token', tokenCnt-1 )
  //         clusterNodes[ cfg.OWN_NODE_ADDR ].token[ t ].handover = true
  //         announceToken = t
  //         // TODO look into sy and ld and try to balance load in cluster
  //       }
  //     }
  //     cnt ++
  //   }
  // }
  // avoid other nodes should not say thrumors about myself
  clusterNodes[ cfg.OWN_NODE_ADDR ].upd = Date.now()
}

function getAnnouncedToken() {
  return announceToken
}
// ===========================================================================


// async function lookupCandidateForTokenHandover( ) {
  // log.warn( 'Check Handover', 'TODO' )
  // let maxToken = 0
  // let maxNode  = null
  // for ( let node in clusterNodes ) {
  //   let tokenAtNode = 0
  //   for ( let token in clusterNodes[ node ].token ) {
  //     if ( ! clusterNodes[ node ].token.handover || clusterNodes[ node ].token.handover === true ) {
  //       tokenAtNode ++
  //     }
  //   }  
  //   if ( tokenAtNode > maxToken ) {
  //     maxToken = tokenAtNode
  //     maxNode  = node
  //   }
  // }
  // let myTokenCnt = 0
  // for ( let token in getTokens() ) {
  //   myTokenCnt ++
  // }

  // if ( isSyncing() && myTokenCnt == 0 && maxToken > 1 ) {
  //   log.info( 'Check Handover', isSyncing(), myTokenCnt, maxToken )
  //   return maxNode
  // } else  { 
  //   return null
  // }
// }


// async function askForTokenHandover( node ) { // TODO: ensure the "ring" is correct
  // if ( isSyncing() ) { return }
  // log.warn( '>>> Request Token Handover:', node, 'TODO' )
  // let reqDta = {
  //   node: cfg.OWN_NODE_ADDR
  // }
  // let handover = await api.get( node, '/cluster/token/handover/', reqDta )
  // log.info( '>>>  Handover' , handover )
  // if ( handover.token ) {
  //   await setMyToken( handover.token, {
  //     sz : 0,    // storage [GB]
  //     ld : 0,    // load [qry/h]
  //     an : node  // assist node, perhaps data is there?
  //   })
  //   // myNodeStatus.token[ handover.token ] = {
  //   //   sz : 0,    // storage [GB]
  //   //   ld : 0,    // load [qry/h]
  //   //   an : node  // assist node, perhaps data is there?
  //   // }
  //   syncTokenStatus[ handover.token ] = { 
  //     status   : 'Transfer fisnished', // TODO 'Init',
  //     fromNode : node
  //   } 

  //   // TODO just for testing
  //   setTimeout( async () =>  { 
  //     let reqDta = {
  //       node        : cfg.OWN_NODE_ADDR,
  //       tokenStatus : { 
  //         status : 'Transfer fisnished' 
  //       }
  //     }
  //     for ( let token in syncTokenStatus ) {
  //       reqDta.tokenStatus.token = token
  //     }
  //     log.info( '###### send handover update', reqDta )
  //     let handover = await api.post( node, '/cluster/token/handover/', reqDta )
  //     log.info( '###### handover update response', handover )
  //     // TODO
  //   }, 10000 )

  //   helper.printNodesShort( 'after handover' )
  // }
// }


// function handoverToken( toNode ) {
//   let announce = getAnnouncedToken()
//   log.info( '<<<<<<  Handover' , announce)
//   if ( isSyncing( toNode ) ) { //don't hand over to a re-joining node
//     return null
//   } 
//   if ( announce ) {
//     if ( myNodeStatus.token[ announce ].handover === true ) {
//       log.info( '<<<<<<  Handover true' )
//       setMyTokenHandover( announce, toNode )
//       // myNodeStatus.token[ announce ].handover = toNode
//       return announce
//     }
//   }
//   return null
// }


// function handoverTokenStatus( node, upd ) {
//   let updates = [ ]
//   if ( isMyTokenInHandOverTo( upd.token, node ) ) {
//     if ( upd.status == 'Transfer fisnished' ) {
//       setStatus( 'OK' )
//       deleteMyToken( upd.token )
//       // delete myNodeStatus.token[ upd.token ]
//       updates.push({
//         token     : upd.token,
//         newStatus : 'Deleted'
//       })
//     }  
//   }
//   return updates
// }

// function isMyTokenInHandOverTo( token, node ) {
//   let tStat = myNodeStatus.token[ token ]
//   if ( tStat && tStat.handover == node ) {
//     return true 
//   } 
//   return false
// }

// async function setMyToken( token, meta ) {
//   myNodeStatus.token[ token ] = meta
//   await writeNodeInfo()
// }

async function setAllMyTokens( tokenMap ) {
  myNodeStatus.token = tokenMap
  await writeNodeInfo()
}

function getOwnTokens( ) {
  return myNodeStatus.token
}

// async function setMyTokenHandover( token, toNode ) {
//   myNodeStatus.token[ token ].handover = toNode
//   await writeNodeInfo()
// }

// async function deleteMyToken( token ) {
//   delete myNodeStatus.token[ token ]
//   await writeNodeInfo()
// }

// ===========================================================================

function handleNodeUnavailable( addr ) {
  log.info( '>>>> unavailable', addr )
  if ( clusterNodes[ addr ].status == 'ERROR CONFIRMED' ) {
    clusterNodes[ addr ].upd = Date.now()
  } else if ( clusterNodes[ addr ].status != 'ERROR' ) {
    clusterNodes[ addr ].status  = 'ERROR' 
    clusterNodes[ addr ]._errorBy = cfg.OWN_NODE_ADDR 
    clusterNodes[ addr ].upd     = Date.now()
  } else {
    // TODO ??
  }
  printNodesShort( 'handleNodeUnavailable' )
}

function setNodeAvailable( addr ) {
  if (  clusterNodes[ addr ] && clusterNodes[ addr ].status.startsWith( 'ERROR' ) ) {
    log.info( '>>>> reset error', addr )
    clusterNodes[ addr ].status = 'Rejoining'
    clusterNodes[ addr ].upd     = Date.now()
    delete clusterNodes[ addr ]._errorBy 
  }
  printNodesShort( 'setNodeAvailable' )
}

// ===========================================================================
// helper 

function getClusterSize() {
  let len = 0
  for ( let node in clusterNodes ) {
    if ( clusterNodes[ node ].nodeId ) {
      len++ 
    }
  }
  return len
}

