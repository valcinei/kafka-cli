
import {
    KafkaClient,
    Producer,
    KafkaClientOptions

} from 'kafka-node';
import { GluegunToolbox } from 'gluegun';

    module.exports = {
        name: 'producer',
        alias: ['p'],
        run: async (toolbox: GluegunToolbox) => {

            const aksHost = { type: 'input', name: 'host', message: 'Whats host you broker?(default:localhost:9092)' }
            const aksTopic = { type: 'input', name: 'topic', message: 'Whats topic you produce?' }
            const  { host, topic } = await toolbox.prompt.ask([aksHost, aksTopic])
            if(host) {
                startProduce({ kafkaHost : host })
            }else{
                startProduce() 
            }
            while (true) {
                const Aksmsg = { type: 'input', name: 'data', message: 'Insert your message:' }
                const  msg = await toolbox.prompt.ask(Aksmsg)
                sendMsg(topic, msg.data)
            }
        }

    }

// tslint:disable-next-line: one-variable-per-declaration
let client = new KafkaClient(),
    producer = new Producer(client)

const sendMsg = (topic, dataMsg) => {

    let payloads = [
        { topic: topic, messages: dataMsg, partition: 0 },
    ];
        producer.send(payloads, (err, dataMsg) => {
            console.log(dataMsg); 
            console.log('Sended')
        });
}

const startProduce = (options?: KafkaClientOptions) => {
    client = new KafkaClient(options);
    producer = new Producer(client);


}