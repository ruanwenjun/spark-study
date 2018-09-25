package com.wenjun.KafkaHelloWorld;


import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 *
 * @Author RUANWENJUN
 * @Creat 2018-09-25 18:07
 */

public class KafkaProducer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        // 这里如果把topic变为带空格的那么就很慢
        String topic = "testkafka";
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "kafka" + i, System.currentTimeMillis() + "");
            producer.send(record);
            System.out.println("生产"+record.key()+"-----"+record.value());
        }
        producer.close();
    }
}
