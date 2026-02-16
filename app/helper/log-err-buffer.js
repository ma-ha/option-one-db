
module.exports = function ErrorBuffer( errorLogs ) {

  const write = function write( r ) {
    const record = typeof r === 'string' ? JSON.parse(r) : r

    if ( record.level >= 40 ) {
      let level = 'WARN'
      if ( record.level >= 50 ) { level = 'ERROR' }
      else if (  record.level >= 60 ) { level = 'FATAL' }
  
      errorLogs.logArr.push({
        t: Date.now(),
        l: level,
        h: record.hostname,
        m: record.msg
      })
      // console.log( '>>', record )

    }
  };

  return { write };
};