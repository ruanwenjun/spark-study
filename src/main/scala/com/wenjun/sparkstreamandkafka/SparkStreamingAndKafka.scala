package com.wenjun.sparkstreamandkafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingAndKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingTest")
    val ssc = new StreamingContext(conf, Seconds(4))
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "streamingtest5",
      "auto.offset.reset" -> "largest",
      // 添加这个配置，当kill spark streaming 进程时，spark streaming不会立即停止，而是把当前的
      // 批处理数据处理完毕后才会停掉，此期间不会再消费kafka里面的数据
      "spark.streaming.stopGracefullyOnShutdown" -> "true")
    val topicSet = Set("log")
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    ds.foreachRDD(line => line.foreach(println))
    ssc.start()
    ssc.awaitTermination()
  }

}
