package com.wenjun.KafkaSendLog;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * 使用kafka发送日志数据，日志数据来源 http://www.sogou.com/labs/resource/tce.php
 *
 * @Author RUANWENJUN
 * @Creat 2018-09-26 10:37
 */

public class KafkaSendLog {

    public static void main(String[] args) throws IOException {
        // 数据来源搜狗
        BufferedReader reader = new BufferedReader(new FileReader("E:\\sougou数据\\SogouQ.reduced"));
        String line;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "log";
        while ((line = reader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            producer.send(record);
            System.out.println("生产" + record.key() + "-----" + record.value());
        }
        producer.close();
        reader.close();
    }
}
