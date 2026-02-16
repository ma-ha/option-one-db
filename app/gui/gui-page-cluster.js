
exports: module.exports = {
  addClusterPage
}

// ============================================================================


function addClusterPage( gui ) {
  let clusterPg = gui.addPage( 'cluster', 'Cluster', null , null )
  clusterPg.navLabel = 'Cluster'
  clusterPg.title    = 'Cluster'
  clusterPg.setPageWidth( '90%' )

  gui.pages['cluster'].dynamicRow(  async ( staticRows, req, page ) => {
    let fillCol   = ( req.xCSS == 'dark' ? '#DDD' : '#333' )
    let strokeCol = ( req.xCSS == 'dark' ? '#333' : '#FFF' )
    const clusterStats = {
      id: 'ClusterLst', rowId: 'ClusterLst', title: '', height: '790px', 
      type:   'pong-list', resourceURL: 'metrics/api',
      moduleConfig: {
        // maxRows:'4',
        rowId: 'podName',
        // pollDataSec: "5",
        divs: [       
          { id:'XPod', cellType:'div',
            divs: [
              { id:'Pod',     cellType:'label', label:'Pod:' },
              { id:'podName', cellType:'text' },
              { id:'Status',  cellType:'label', label:'Status:' },
              { id:'status',  cellType:'text' },
              { id:'NodeId',  cellType:'label', label:'ID:' },
              { id:'nodeId',  cellType:'text' },
              { id:'Tokens',  cellType:'label', label:'Token:' },
              { id:'tokens',  cellType:'text' }
            ]
          },
          { id: 'XUsage', cellType: 'div', 
            divs : [ 
                { id : 'reqPM', cellType: 'graph',
                  layout:{
                    name: 'API requests 1d [req / min]',
                    graphType: 'timeLog',
                    colors: { 
                      GET: '75A5', POST: '#5A7', DELETE: '#3A9', PUT: '#1A8', sync: '#999', 
                      QMSGIN:'#F0A', QMSGOUT:'#F07', QJOBIN:'#F70', QJOBOUT:'#FA0'
                    },
                    yAxis: {
                      axisType : 'linear',
                      min      : '0',
                      max      : 'auto',
                      labelCnt : 3
                    }
                  },
                  fillCol : fillCol,
                  textFillColor : strokeCol,
                  textStrokeColor : fillCol,
                  borderCol : strokeCol
                } 
              ]
          }
        ]
      }
    }

    return [ clusterStats ]
  })
}
