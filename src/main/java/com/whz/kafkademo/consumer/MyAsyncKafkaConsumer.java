package com.whz.kafkademo.consumer;

import com.whz.kafkademo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MyAsyncKafkaConsumer {
    private static final int minBatchSize = 30;
    private static List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConfig.kafkaServer);
        props.put("group.id", KafkaConfig.groupId);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(KafkaConfig.topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("async:offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.println("async reached buffer size");
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

}