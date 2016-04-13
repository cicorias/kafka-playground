'use strict';

var kafka = require('kafka-node');
var Client = kafka.Client;
var Offset = kafka.Offset;

var argv = require('minimist')(process.argv.slice(2));
var host = argv.host || 'localhost';
var port = argv.port || '2181';

var cluster = host + ':' + port;

var offset = new Offset(new Client(cluster));

var topic = argv.topic || 'topic';


// Fetch available offsets
offset.fetch([
    { topic: topic, partition: 0, maxNum: 2 },
    { topic: topic, partition: 1 },
], function (err, offsets) {
    console.log(err || offsets);
});

// Fetch commited offset
offset.commit('kafka-node-group', [
    { topic: topic, partition: 0 }
], function (err, result) {
    console.log(err || result);
});

