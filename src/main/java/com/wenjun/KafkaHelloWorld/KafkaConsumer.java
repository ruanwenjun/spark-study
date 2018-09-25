package com.wenjun.KafkaHelloWorld;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * kafka消费者
 *
 * @Author RUANWENJUN
 * @Creat 2018-09-25 18:34
 */

public class KafkaConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "hello kafka2");
        properties.put("auto.offset.reset", "earliest");
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);
        List<String> topic = new ArrayList<>();
        topic.add("testkafka");
        consumer.subscribe(topic);
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("消费：" + record.key() + "----------" + record.value());
        }
    }
}
