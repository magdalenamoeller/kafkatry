package com.zanox;

import groovy.ui.Console;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import groovy.lang.Binding;

public class DemoProducer {

    private Producer<String, byte[]> producer = new Producer<String, byte[]>(configure());

    public static void main(String args[]) {
        DemoProducer producer = new DemoProducer();

        // Generates a few notifications
        producer.run();

        // Console window to play with "produces"
        Binding binding = new Binding();
        binding.setVariable("producer", producer);
        Console console = new Console(binding);
        console.run();
    }

    private ProducerConfig configure() {
        Properties pros = new Properties();
        //pros.put("metadata.broker.list", "s-kafka-01.zanox.com:9092");
        pros.put("metadata.broker.list", "s-ber1-kafka-011.zanox.com:9092");
        pros.put("request.required.acks", "1");

        return new ProducerConfig(pros);
    }

    private void run() {
        for(User.Notification notification : generateNotificationList(5)) {
            System.out.println("Generated: " + notification);
            producer.send(new KeyedMessage<String, byte[]>("trainingKafka3ada", notification.toByteArray()));
        }

    }

    public void sendNotification(String from, String to, String body) {
      producer.send(new KeyedMessage<String, byte[]>("trainingKafka3ada", generateNotification(from, to, body).toByteArray()));
    }

    private static Iterable<User.Notification> generateNotificationList(int n) {
        List<User.Notification> notifications = new ArrayList<User.Notification>();
        for(int i = 0; i < n; i++) {
            notifications.add(generateNotification("System", "whoeever@service.com", "Hello" + i));
        }
        return notifications;
    }

    private static User.Notification generateNotification(String from, String to, String body) {
      return User.Notification.newBuilder()
              .setMessageId(UUID.randomUUID().toString())
              .setFromUser(from)
              .setToUser(to)
              .setBody(body)
              .setSendOn(System.currentTimeMillis())
              .build();
    }
}
