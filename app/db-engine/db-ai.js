const log    = require( '../helper/logger' ).log
const cfgHlp = require( '../helper/config' )
const axios  = require( 'axios' )


module.exports = {
  init,
  getEmbedding,
  cosSimilarity
}

let cfg = {
  EMBEDDING_GEMMA_API: null
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
}


async function getEmbedding( txnId, model, text ) {
  let embeddings = null
  try {
    let gemmaResult = await axios.post(
      cfg.EMBEDDING_GEMMA_API,
      {
        model: model,
        input: text
      }
    )
    if ( gemmaResult.status == 200 ) {
      embeddings = gemmaResult.data.embeddings
    }

  } catch ( exc ) {
    log.error( txnId, cfg.EMBEDDING_GEMMA_API, exc.message )
  }
  return embeddings
}

// let gemmaResult = await axios.post( "http://192.178.178.126:11434/api/embed", { model: 'embeddinggemma', input: 'this is a test' } )


function cosSimilarity( x, y ) {
  if ( ! x || ! y || x.length != y.length ) { return 0 }
  let xLen = 0
  let yLen = 0
  let xy = 0
  for ( let i = 0; i < x.length; i++ ) {
    xy += x[i] * y[i]
    xLen +=  x[i] * x[i]
    yLen +=  y[i] * y[i]
  }
  xLen = Math.sqrt( xLen )
  yLen = Math.sqrt( yLen )
  let similarity = xy / ( xLen * yLen )
  log.debug( 'cosSimilarity', similarity, xy, xLen, yLen)
  return similarity
}