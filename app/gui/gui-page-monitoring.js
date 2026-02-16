exports: module.exports = {
  addMonitoringPage
}

// ============================================================================

function addMonitoringPage( gui, logExpireDays ) {
  let monitoring = gui.addPage( 'monitoringPage', 'Monitoring', null , null )
  monitoring.navLabel = 'Monitoring'
  monitoring.title    = 'Monitoring'
  monitoring.setPageWidth( '90%' )

  gui.pages['monitoringPage'].dynamicRow( async ( staticRows, req, page ) => {
    let pageCols = []

    pageCols.push( {
      columnId: "monitoringCol1", width: "100%", height: "700px",
      rows: [
        {
          title: "Error Logs (expiry: "+logExpireDays+' days)',
          id: "logTbl", rowId: "logTbl",
          height: "690px", decor: "decor",
          type: "pong-table",
          moduleConfig: {
            dataURL : "",
            filter:{
              dataReqParams: [ 
                { id:'log', label:'Log', type:'text' },
                { id:'sv', label:'Severity', type:'select',
                   options:[
                    { option:'*', value:'*' },
                    { option:'ERROR', value:'ERROR' },
                    { option:'FATAL', value: 'FATAL' }
                ]},
                { id:'dt', label:' Date', type: 'date' }
              ],
              dataReqParamsSrc: 'Form'
            },
            pollDataSec: "60",
            rowId   : ['id'],
            cols    : [
              { id: "dt", label: "Date",    cellType: "text", width: "8%" },
              { id: "l", label: "Severity", cellType: "text", width: "5%" },
              { id: "h", label: "Host",     cellType: "text", width: "10%" },
              { id: "m", label: "Log",      cellType: "text", width: "77%" },
            ]
          },
          resourceURL: "log"
        },
        {
          title: "Monitoring Configuration",
          id: "MonConfigFrm", rowId: "MonConfigFrm",
          height: "110px", decor: "decor",
          type: 'pong-form', 
          moduleConfig: {
            fieldGroups: [{
              columns: [
                { formFields: [
                  { id: 'metricsEnabled', type: 'checkbox', label: 'Metrics exporter enabled', readonly: true },
                ] }
              ] 
            }],
            actions : [ 
              { id: 'OnInit', onInit: { getInitValues: 'defaultValues' }, method: 'GET' }
            ]
          },
          resourceURL: "monitoring"
        }
      ]
    })

    return [{ 
      id: 'monitoringRow1', rowId: "monitoringRow1",
      // title: 'Dynamic View '+i,
      height: '800px', 
      cols : pageCols
    }]
  })

}
