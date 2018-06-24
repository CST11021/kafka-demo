package com.whz.kafkademo;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@SpringBootApplication
@Slf4j
public class KafkaDemoApplication implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> template;

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // 服务启动时会发送以下消息
        this.template.send("myTopic1", "foo1");
        this.template.send("myTopic1", "foo2");
        this.template.send("myTopic1", "foo3");
        this.template.send("myTopic2", "key1", "foo4");
        this.template.send("myTopic2", "key2", "foo5");

        String line;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while ((line = readLine(reader)) != null) {
            this.template.send("defaultTopic", line);
        }
    }


    @KafkaListener(topics = {"myTopic1", "myTopic2", "defaultTopic"})
    public void listen(ConsumerRecord<?, ?> cecord) throws Exception {
        System.out.println(cecord.toString());
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}
