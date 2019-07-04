
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
            const aksTopic = { type: 'input', name: 'topics', message: 'Whats topic you want produce?(separated by comma)' }
            const aksPartition = { type: 'input', name: 'partition', message: 'How many partitions?(default: 0)' }
            const  { host, topics, partitions } = await toolbox.prompt.ask([aksHost, aksTopic,aksPartition])
            const topicList = topics.split(',')

            if(host) {
                startProduce({ kafkaHost : host })
            }else{
                startProduce() 
            }
            while (true) {
                const Aksmsg = { type: 'input', name: 'data', message: 'Insert your message:' }
                const  msg = await toolbox.prompt.ask(Aksmsg)
                sendMsg(topicList, msg.data, partitions)
            }
        }

    }

// tslint:disable-next-line: one-variable-per-declaration
let client = new KafkaClient(),
    producer = new Producer(client)

const sendMsg = (topicList, dataMsg, partitions) => {
    console.log(partitions)
    let payloads = [ ];
    topicList.forEach(topic => {        
        payloads.push({ topic: topic, messages: dataMsg, partition: partitions?partitions:0 })
    });
        producer.send(payloads, (err, dataMsg) => {
            console.log(dataMsg); 
            console.log('Sended')
        });
}

const startProduce = (options?: KafkaClientOptions) => {
    client = new KafkaClient(options);
    producer = new Producer(client);


}