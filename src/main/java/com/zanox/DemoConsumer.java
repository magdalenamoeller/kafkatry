package com.zanox;

import com.google.protobuf.InvalidProtocolBufferException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DemoConsumer implements Runnable{

    ConsumerConnector connector = null;

    public static void main(String argsp[]) {        
        DemoConsumer consumer = new DemoConsumer();
        Thread t = new Thread(consumer);
        t.start();
        System.out.println("Started. Reading from Kafka...");
        try {
            Thread.sleep(500000);
            System.out.println("...Done");
        } catch (InterruptedException ex) {
            // ignore
        }
        consumer.shutdown();
    }


    public ConsumerConfig configure() {
        Properties props = new Properties();
        //props.setProperty("zookeeper.connect", "s-zk-01:2181");
        props.setProperty("zookeeper.connect", "s-ber1-zk-011.zanox.com:2181");
        props.setProperty("group.id","group1");
        props.setProperty("auto.commit.enable","false");
        props.setProperty("auto.offset.reset","smallest");
        return new ConsumerConfig(props);
    }
    
    @Override
    public void run() {
        connector = Consumer.createJavaConsumerConnector(configure());
        Map<String, Integer> topicClients = new HashMap<>();
        topicClients.put("trainingKafka3ada", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicsStreams = connector.createMessageStreams(topicClients);
        List<KafkaStream<byte[], byte[]>> topicStreams = topicsStreams.get("trainingKafka3ada");
        KafkaStream<byte[], byte[]> stream = topicStreams.get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            try {
                User.Notification message = User.Notification.parseFrom(it.next().message());
                System.out.println(message.toString());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
    
    protected void shutdown() {
        if(connector != null) {
            connector.shutdown();
        }
    }
}
