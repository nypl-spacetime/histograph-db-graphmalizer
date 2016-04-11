'use strict'

var H = require('highland')
var Graphmalizer = require('graphmalizer-core')
var config = require('spacetime-config')
var schemas = require('spacetime-schemas')

var neo4jAuth
if (config.neo4j.user && config.neo4j.password) {
  neo4jAuth = config.neo4j.user + ':' + config.neo4j.password
}

var graphmalizerConfig = {
  types: schemas.graphmalizer,
  Neo4J: {
    hostname: config.neo4j.host,
    port: config.neo4j.port,
    auth: neo4jAuth
  },
  batchSize: config.core.batchSize,
  batchTimeout: config.core.batchTimeout
}

var graphmalizer = new Graphmalizer(graphmalizerConfig)

var ACTION_MAP = {
  create: 'add',
  update: 'add',
  delete: 'remove'
}

function getUnixTime (date) {
  return new Date(date).getTime() / 1000
}

module.exports.initialize = function () {
  // TODO: create unique index!
}

function toGraphmalizer (message) {
  var node = message.payload

  // dataset is a meta attribute that we want copied into the 'data' attribute
  node.dataset = message.meta.dataset

  // Add UNIX timestamps
  if (node.validSince) {
    node.validSinceTimestamp = getUnixTime(node.validSince[0])
  }

  if (node.validUntil) {
    node.validUntilTimestamp = getUnixTime(node.validUntil[1])
  }

  return {
    operation: ACTION_MAP[message.action],
    dataset: node.dataset,
    type: node.type,

    // nodes are identified with IDs or URIs, we don't care
    id: node.id,
    source: node.from,
    target: node.to,

    // Only pass name and dates to Graphmalizer
    data: {
      name: node.name,
      validSince: node.validSinceTimestamp,
      validUntil: node.validUntilTimestamp
    }
  }
}

function logError (err) {
  console.error(err.stack || err)
}

// TODO: implement bulk function!!!
// module.exports.bulk = function (messages, callback) {
//   var done = false
//   var stream = H(messages)
//
//   // TODO: this is NOT the right way to do this, but don't know better solution...
//   fromStream(stream)
//     .errors(callback)
//     .append(H.nil)
//     .each(() => {
//       if (!done) {
//         callback()
//       }
//       done = true
//     })
//
//     // shutdown
// }

function fromStream (stream) {
  var graphmalizerStream = H(stream)
    .errors(logError)
    .filter((message) => message.type === 'pit' || message.type === 'relation')
    .map(toGraphmalizer)
    .errors(logError)

  return graphmalizer.register(graphmalizerStream)
}

module.exports.fromStream = fromStream
