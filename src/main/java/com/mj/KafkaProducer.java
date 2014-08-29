package com.mj;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		 
                // List of brokers that the producer will try to connect to
                props.put("metadata.broker.list", "192.168.33.21:9092,192.168.33.22:9092");
                // Since we are sending text messages, use StringEncoder
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// Consider publish message to be completed when you receive an acknoledgement from the leader
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		
                // Create Producer that will be sending messages to Kafka brokers
		Producer<String, String> producer = new Producer<String, String>(config);
		
		// Get a list of topics by running following command
                // $KAFKA_HOME/bin/kafka-list-topic.sh --zookeeper 192.168.33.21:2181
//                [2014-08-29 15:31:30,208] INFO zookeeper state changed (SyncConnected) (org.I0Itec.zkclient.ZkClient)
//                topic: korb_test	partition: 0	leader: 1	replicas: 1	isr: 1
//                topic: my_test_topic	partition: 0	leader: 2	replicas: 2	isr: 2
//                topic: topic-1	partition: 0	leader: 1	replicas: 1	isr: 1
                // if no topics exist, below code will create one for you
                String topic = "my_test_topic" ;		
		
		for (int i = 1 ; i <= 10 ; i++) {
			
                    String msg = "Test message # " + i ;
                    System.out.println("About to send '" + msg + "' message to Kafka!") ;

                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(i), msg);
                    producer.send(data);
		}
		// Use following command to verify that messages were received
                // vagrant@debian-70rc1-x64-vbox4210:~$ sudo $KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper 192.168.33.21:2181 --topic my_test_topic  --from-beginning
                // You should see something like this
//                [2014-08-29 15:28:39,734] INFO [ConsumerFetcherManager-1409326119149] Adding fetcher for partition [my_test_topic,0], initOffset -1 to broker 2 with fetcherId 0 (kafka.consumer.ConsumerFetcherManager)
//                Test message # 1
//                Test message # 2
//                Test message # 3
//                Test message # 4
//                Test message # 5
//                Test message # 6
//                Test message # 7
//                Test message # 8
//                Test message # 9
//                Test message # 10
		producer.close();
	}
}
