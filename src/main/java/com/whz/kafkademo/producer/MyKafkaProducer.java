package com.whz.kafkademo.producer;

import com.whz.kafkademo.config.KafkaConfig;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) throws IOException{
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConfig.kafkaServer);
        properties.put("acks", "all");
        properties.put("retries", 0);
        //properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(properties);
        String topic = KafkaConfig.topic;


        int messageNo = 1;
        String messageBody;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while ((messageBody = readLine(reader)) != null) {
            System.out.println("Send:" + messageBody);
            producer.send(new ProducerRecord<>(topic, "key_" + messageNo, messageBody), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println("The offset of the record is: " + metadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
            messageNo++;
        }
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        return reader.readLine();
    }
}