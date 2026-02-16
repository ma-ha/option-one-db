exports: module.exports = {
  init
}

// ============================================================================

function init( gui, appName ) {
  gui.pages['main'].setPageWidth( '90%' )
  gui.pages['main'].navLabel = 'Metrics'
  gui.pages['main'].title = appName

  gui.pages['main'].dynamicRow( async ( staticRows, req, page ) => {
    let userId = await gui.getUserIdFromReq( req )
    if ( ! userId ) {

      return [{ 
        id     : 'colRow1', 
        height : '800px', 
        decor  : "decor",
        resourceURL: "gui/static/welcome"
      }]

    } else {
      let fillCol   = ( req.xCSS == 'dark' ? '#DDD' : '#333' )
      let strokeCol = ( req.xCSS == 'dark' ? '#333' : '#FFF' )
  
      let tabs = []

      const dbStats = {
        id: 'DbLst', rowId: 'DbLst', title:  '', height: '790px', 
        type: 'pong-list',resourceURL: 'metrics/db',
        moduleConfig: {
          // maxRows:'4',
          rowId: 'ID',
          // pollDataSec: "5",
          divs: [       
            { id:'Xdb', cellType:'div',
              divs: [
                { id:'name', cellType:'text' },
              ]
            },
            { id: 'XDbMetrics', cellType: 'div', 
              divs : [ 
                  { id : 'reqDB', cellType: 'graph',
                    layout:{
                      name: 'Operations / hour (1d)',
                      graphType: 'timeLog',
                      colors: { 
                        ins: '#009', err: '#900', fnd : '#3A9', 
                        upd: '#0B0', del: '#666'
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
                    borderCol: strokeCol
                  } 
                ]
            }
          ]
        }
      }

      tabs.push( dbStats)

      return tabs
    }
  })
}
