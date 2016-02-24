var H = require('highland')
var Graphmalizer = require('graphmalizer-core')
var config = require('histograph-config')
var schemas = require('histograph-schemas')
var normalize = require('histograph-uri-normalizer').normalize
var fuzzyDates = require('fuzzy-dates')

var neo4jAuth
if (config.neo4j.user && config.neo4j.password) {
  neo4jAuth = config.neo4j.user + ':' + config.neo4j.password
}

var conf = {
  types: schemas.graphmalizer,
  Neo4J: {
    hostname: config.neo4j.host,
    port: config.neo4j.port,
    auth: neo4jAuth
  },
  batchSize: config.core.batchSize,
  batchTimeout: config.core.batchTimeout
}

var graphmalizer = new Graphmalizer(conf)

var ACTION_MAP = {
  add: 'add',
  update: 'add',
  delete: 'remove'
}

function getUnixTime (date) {
  return new Date(date).getTime() / 1000
}

// when passed an object, every field that contains an object
// is converted into a JSON-string (*mutates in place*)
function stringifyObjectFields (obj) {
  // convert objects to JSONified strings
  var d = JSON.parse(JSON.stringify(obj))

  if (typeof (d) === 'object') {
    Object.keys(d).forEach(function (k) {
      var v = d[k]

      if (v.constructor === Object) {
        d[k] = JSON.stringify(v)
      }
    })
  }

  return d
}

function toGraphmalizer (msg) {
  function norm (x) {
    if (x) {
      return normalize(x, msg.dataset)
    }

    return undefined
  }

  var d = msg.data || {}

  // dataset is a top-level attribute that we want copied into the 'data' attribute
  d.dataset = msg.dataset

  // Parse fuzzy dates to arrays using fuzzy-dates module
  if (d.validSince) {
    d.validSince = fuzzyDates.convert(d.validSince)

    // Add timestamp
    // TODO: find more structured way to add extra values/fields
    //   to Graphmalizer (and Neo4j afterwards) - API needs to remove
    //   those fields later
    d.validSinceTimestamp = getUnixTime(d.validSince[0])
  }

  if (d.validUntil) {
    d.validUntil = fuzzyDates.convert(d.validUntil)

    // Add timestamp
    d.validUntilTimestamp = getUnixTime(d.validUntil[1])
  }

  return {
    operation: ACTION_MAP[msg.action],
    dataset: d.dataset,
    type: d.type,

    // nodes are identified with IDs or URIs, we don't care
    id: norm(d.id || d.uri),

    // normalize source/target id's
    source: norm(d.from),
    target: norm(d.to),
    data: stringifyObjectFields(d)
  }
}

function logError (err) {
  console.error(err.stack || err)
}

module.exports.fromStream = function (stream) {
  var graphmalizerStream = H(stream)
    .errors(logError)
    .map(toGraphmalizer)

  graphmalizer.register(graphmalizerStream).done(() => {})
}
