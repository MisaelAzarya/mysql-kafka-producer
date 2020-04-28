const mysql = require('mysql');
const MySQLEvents = require('@rodrigogs/mysql-events');
const kafka = require('kafka-node');
const config = require('./config');


const program = async () => {
    const connection = mysql.createConnection({
        // untuk localhost
        // host: 'localhost',
        // untuk localhost dari docker
        host: "192.168.65.2",
        user: 'root',
        password: '',
        port: "3306",
    });
    
    const instance = new MySQLEvents(connection, {
        startAtEnd: true // to record only the new binary logs, if set to false or you didn'y provide it all the events will be console.logged after you start the app
    });
    instance.start();
    console.log("instance")

    instance.addTrigger({
        name: 'monitoring all statments',
        expression: 'supersample.*', // listen to supersample database !!!
        statement: MySQLEvents.STATEMENTS.ALL, // you can choose only insert for example MySQLEvents.STATEMENTS.INSERT, but here we are choosing everything
        onEvent: logs => {
            console.log(logs);
            var parseobj = JSON.stringify(logs);

            try {
                const Producer = kafka.Producer;
                const client = new kafka.KafkaClient(config.kafka_server);
                const producer = new Producer(client);
                const kafka_topic = config.kafka_topic;
                let payloads = [
                    {
                    topic: kafka_topic,
                    messages: parseobj
                    }
                ];
                
                producer.on('ready', async function() {
                    let push_status = producer.send(payloads, (err, data) => {
                    if (err) {
                        console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
                    } else {
                        console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
                    }
                    });
                });
                
                producer.on('error', function(err) {
                    console.log(err);
                    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
                    throw err;
                });
            }
            catch(e) {
                console.log(e);
            }
        }
    });

    instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
    instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
};

program();
