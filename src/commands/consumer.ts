
import {
    KafkaClient,
    Consumer,
    KafkaClientOptions,
    OffsetFetchRequest

} from 'kafka-node';
import { GluegunToolbox } from 'gluegun';

module.exports = {
    name: 'consumer',
    alias: ['c'],
    run: async (toolbox: GluegunToolbox) => {

        const aksHost = { type: 'input', name: 'host', message: 'Whats host you broker?(default:localhost:9092)' }
        const aksTopic = { type: 'input', name: 'topics', message: 'Whats topic you want consume?(separated by comma)' }
        const aksPartition = { type: 'input', name: 'partition', message: 'How many partitions?(default: 0)' }
        const { host, topics, partitions } = await toolbox.prompt.ask([aksHost, aksTopic, aksPartition])

        let fetchRequestsList: Array<OffsetFetchRequest> = []
        const topicList = topics.split(',')

        if (topicList && topicList.length > 0) {
            topicList.forEach((topic: any) => {
                let newTopic = {
                    topic: topic,
                    partition:partitions?Number(partitions):0
                }
                fetchRequestsList.push(newTopic)

            })
        } else {
            console.log('Please insert your topics');
            return
        }

        if (host) {
            startConsumer(fetchRequestsList, { kafkaHost: host })
        } else {
            startConsumer(fetchRequestsList)
        }
    }

}


const startConsumer = (topicList?: Array<OffsetFetchRequest>, options?: KafkaClientOptions, autoCommit = false) => {
    const client = new KafkaClient(options);
    const consumer = new Consumer(client, topicList, { autoCommit: autoCommit });
    consumer.on('message', function (message) {
        console.log(message);
    });

}