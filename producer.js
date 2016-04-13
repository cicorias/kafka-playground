var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;
var argv = require('minimist')(process.argv.slice(2));

var host = argv.host || 'localhost';
var port = argv.port || '2181';


var cluster = host + ':' + port;

var client = new Client(cluster);
var argv = require('optimist').argv;
var topic = argv.topic || 'topic';
var p = argv.p || 0;
var a = argv.a || 0;
var producer = new Producer(client, { requireAcks: 1 });

producer.on('ready', function () {
    var message = 'a message';
    var keyedMessage = new KeyedMessage('keyed', 'a keyed message');

    producer.send([
        { topic: topic, partition: p, messages: [message, keyedMessage], attributes: a }
    ], function (err, result) {
        console.log(err || result);
        process.exit();
    });
});

producer.on('error', function (err) {
    console.log('error', err)
});