'use strict';

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('minimist')(process.argv.slice(2));
var topic = argv.topic || 'topic';
var host = argv.host || 'localhost';
var port = argv.port || '2181';


var cluster = host + ':' + port;

console.log('Connecting to: %s', cluster);
var client = new Client( cluster );
var topics = [
        {topic: topic, partition: 3},
        {topic: topic, partition: 2},
        {topic: topic, partition: 1},
        {topic: topic, partition: 0}
    ];

var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on('error', function (err) {
    console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
    topic.maxNum = 2;
    offset.fetch([topic], function (err, offsets) {
        var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
        consumer.setOffset(topic.topic, topic.partition, min);
    });
});

