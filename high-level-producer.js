var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var Client = kafka.Client;

var argv = require('minimist')(process.argv.slice(2));
var topic = argv.topic || 'topic';
var host = argv.host || 'localhost';
var port = argv.port || '2181';

var cluster = host + ':' + port;
var client = new Client(cluster);

var count = 10, rets = 0;
var producer = new HighLevelProducer(client);

producer.on('ready', function () {
    setInterval(send, 1000);
});

producer.on('error', function (err) {
    console.log('error', err)
})

function send() {
    var message = new Date().toString();
    producer.send([
      {topic: topic, messages: [message] }
    ], function (err, data) {
        if (err) console.log(err);
        else console.log('sent %d messages', ++rets);
        if (rets === count) process.exit();
    });
}

