package com.whz.kafkademo.config;

public interface KafkaConfig {
    String zkConnect = "127.0.0.1:2181";
    String groupId = "group1";
    String topic = "topic1";
    String kafkaServer = "localhost:9092";
    int kafkaProducerBufferSize = 64 * 1024;
    int connectionTimeOut = 20000;
    int reconnectInterval = 10000;
    String topic2 = "topic2";
    String topic3 = "topic3";
    String clientId = "SimpleConsumerDemoClient";
}