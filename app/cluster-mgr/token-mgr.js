const log      = require( '../helper/logger' ).log
const statMgr  = require( './node-status-mgr' ) 
const pubsub   = require( './pubsub' )

module.exports = {
  init,
  tasksToAddNode,
  nextToken,
  nodeMinus1,
  nodeMinus2
}

function init() {
  genSlave()
}


const TKN_CONCEPT = [
  {},
  { '0': { m: ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'] } },

  { '0': { n: '1', m: ['0','3','6','8','9','c','e','f'] },
    '1': { n: '0', m: ['1','2','4','5','7','a','b','d'] } },

  { '0': { n: '1', m: ['0','3','6','9','c','f'] },
    '1': { n: '2', m: ['1','7','a','d'] },
    '2': { n: '0', m: ['2','4','5','8','b','e'] } },

  { '0': { n: '1', m: ['0','6','8','c'] },
    '1': { n: '2', m: ['1','9','d','e'] },
    '2': { n: '3', m: ['2','5','7','a'] },
    '3': { n: '0', m: ['3','4','b','f'] } },

  { '0': { n: '1', m: ['0','6','8'] },
    '1': { n: '2', m: ['1','9','7','d'] },
    '2': { n: '3', m: ['2','a','e'] },
    '3': { n: '4', m: ['3','5','b'] },
    '4': { n: '0', m: ['4','c','f'] } },

  { '0': { n: '1', m: ['0','6','8'] },
    '1': { n: '2', m: ['1','7','9'] },
    '2': { n: '3', m: ['2','a'] },
    '3': { n: '4', m: ['3','b'] },
    '4': { n: '5', m: ['4','c','e'] },
    '5': { n: '0', m: ['5','d','f'] } },

  { '0': { n: '1', m: ['0','7','8'] },
    '1': { n: '2', m: ['1','9'] },
    '2': { n: '3', m: ['2','a'] },
    '3': { n: '4', m: ['3','b'] },
    '4': { n: '5', m: ['4','c'] },
    '5': { n: '6', m: ['5','d'] },
    '6': { n: '0', m: ['6','e','f'] } }, 

  { '0': { n: '1', m: ['0','8'] },
    '1': { n: '2', m: ['1','9'] },
    '2': { n: '3', m: ['2','a'] },
    '3': { n: '4', m: ['3','b'] },
    '4': { n: '5', m: ['4','c'] },
    '5': { n: '6', m: ['5','d'] },
    '6': { n: '7', m: ['6','e'] },
    '7': { n: '0', m: ['7','f'] } }, 
]


function genSlave() {
  // add slave array for all cluster nodes
  for ( let cluster of TKN_CONCEPT ) {
    for ( let nodeId in cluster ) {
      cluster[ nodeId ].s = []
    }
  }
  // calculate slaves for size == 2
  let cluster2 = TKN_CONCEPT[ 2 ]
  for ( let nodeId in cluster2 ) {
    let nodePls1 = cluster2[ nodeId ].n
    for ( let token of cluster2[ nodeId ].m ) {
      cluster2[ nodePls1 ].s.push( token )
    }
  }
  // calculate slaves for size >= 3
  for ( let clusterSize of [3,4,5,6,7,8] ) {
    let cluster = TKN_CONCEPT[ clusterSize ]
    for ( let nodeId in cluster ) {
      let nodePls1 = cluster[ nodeId ].n
      let nodePls2 = cluster[ nodePls1 ].n
      for ( let token of cluster[ nodeId ].m ) {
        cluster[ nodePls1 ].s.push( token )
        cluster[ nodePls2 ].s.push( token )
      }
    }
  }
  // for ( let cluster of TKN_CONCEPT ) console.log( cluster )
}


function nodes( count ) {
  return TKN_CONCEPT[ count ]
}

function hasMaster( nCount, node, token ) {
  return  TKN_CONCEPT[ nCount ][ node ]?.m.includes( token )
}
function hasSlave( nCount, node, token ) {
  return  TKN_CONCEPT[ nCount ][ node ]?.s.includes( token )

}


async function tasksToAddNode( podName, addNodeId ) {
  log.info( 'TOKEN MGR: tasksToAddNode', podName, addNodeId, '***************************************************' )
  if ( ! ['3','4','5','6','7'].includes( addNodeId+'' ) ) { 
    log.error( 'tasksToAddNode: Cannot onboard node id ', addNodeId )
    return {} 
  }
  
  await pubsub.sendJob( 'StartOnboarding', {
    jobId    : 'TOK.' + randomChar( 10 ),
    job      : 'StartOnboarding',
    podName  : podName, 
    nodeId   : addNodeId
  })

  // first one job for masterData
  let masterDataJob = {
    jobId    : 'TOK.' + randomChar( 10 ),
    job      : 'TransferTokenData',
    action   : 'CopyMasterData',
    fromNode : '2',  // generally not much utilized
    toNode   : addNodeId
  }
  await pubsub.sendJob( 'TransferTokenData', masterDataJob )


  let clusterSize = Number.parseInt( addNodeId ) + 1
  let action = {}
  for ( let token of ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'] ) {
    action[ token ] = { addToNode:[], rmFrmNode:[], masterNode: null, replicaNode:[] }
  }

  let cluster = nodes( clusterSize )
  for ( let node in cluster ) {

    for ( let token of ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'] ) {
      
      let prior = ' '
      if ( hasMaster( clusterSize - 1, node, token ) ) { prior = 'M' } else 
      if ( hasSlave(  clusterSize - 1, node, token ) ) { prior = 'S' }

      if ( hasMaster( clusterSize, node, token ) ) {
        action[ token ].masterNode = node
        if ( prior == 'S' ) {
          // action[ token ].chgMaster = node
        } else if ( prior == ' ' ) {
          action[ token ].addToNode.push( node )
          // action[ token ].chgMaster = node
        }
      } else if ( hasSlave( clusterSize, node, token ) ) {
        action[ token ].replicaNode.push( node )
        if ( prior == ' ' ) {
          action[ token ].addToNode.push( node )
        }
      } else {
        if ( prior == 'M' ) { 
          action[ token ].rmFrmNode.push( node )
        } else if ( prior == 'S' ) { 
          action[ token ].rmFrmNode.push( node )
        }
      }
    }
    
  }

  // send out jobs
  for ( let token in action ) {
    if ( action[ token ].addToNode.length > 0 &&  action[ token ].rmFrmNode.length > 0 ) {
      let addToNode =  action[ token ].addToNode.pop()
      let rmFrmNode =  action[ token ].rmFrmNode.pop()
      let job = {
        jobId    : 'TOK.' + randomChar( 10 ),
        job      : 'TransferTokenData',
        action   : 'MoveData',
        token    : token,
        fromNode : rmFrmNode,
        toNode   : addToNode
      }
      if ( action[ token ].addToNode.length == 0 ) { // if all done for this token, redefine replication
        job. master = action[ token ].masterNode,
        job.replica = action[ token ].replicaNode
      }
      await pubsub.sendJob( 'TransferTokenData', job )
    }
  }

  return action
}


function nextToken( token ) {
  let helper = statMgr.getHelper()
  let i = parseInt( token, 16 )
  i++
  i = i % helper.getMaxTokenCnt()
  let nextToken =  Number( i ).toString( 16 )
  return nextToken
}

// ============================================================================
// internal helper


function nodeMinus1( id ) {
  let nodeCount = statMgr.getClusterSize()
  let idInt = parseInt( id )
  let nodeId = ( idInt  + nodeCount - 1 ) % nodeCount
  return nodeId + ''
}

function nodeMinus2( id ) {
  let nodeCount = statMgr.getClusterSize()
  let idInt = parseInt( id )
  let nodeId = ( idInt + nodeCount - 2 ) % nodeCount
  return nodeId + ''
}



function randomChar( len ) {
  var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  var token =''
  for ( var i = 0; i < len; i++ ) {
    var iRnd = Math.floor( Math.random() * chrs.length )
    token += chrs.substring( iRnd, iRnd+1 )
  }
  return token
}