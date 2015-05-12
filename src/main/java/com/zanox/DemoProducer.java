package com.zanox;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class DemoProducer {


    public static void main(String args[]) {
        DemoProducer producer = new DemoProducer();
        producer.run();
    }

    private ProducerConfig configure() {
        Properties pros = new Properties();
        //pros.put("metadata.broker.list", "s-kafka-01.zanox.com:9092");
        pros.put("metadata.broker.list", "s-ber1-kafka-011.zanox.com:9092");
        pros.put("request.required.acks", "1");

        return new ProducerConfig(pros);
    }

    private void run() {
        ProducerConfig config = configure();

        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        for(User.Notification notification : generateNotifications(5)) {
            System.out.println("notification: " + notification);
            producer.send(new KeyedMessage<String, byte[]>("trainingKafka3ada", notification.toByteArray()));
        }

    }

    private static Iterable<User.Notification> generateNotifications(int n) {
        List<User.Notification> notifications = new ArrayList<User.Notification>();
        for(int i = 0; i < n; i++) {
            notifications.add(
                    User.Notification.newBuilder()
                            .setMessageId(UUID.randomUUID().toString())
                            .setFromUser("System")
                            .setToUser("whoeever@service.com")
                            .setBody("Hello " + i)
                            .setSendOn(System.currentTimeMillis())
                            .build());
        }
        return notifications;
    }
}
