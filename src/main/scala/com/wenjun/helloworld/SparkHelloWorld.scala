package com.wenjun.helloworld

import org.apache.spark.{SparkConf, SparkContext}

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHelloWorld").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\RUANWENJUN\\Desktop\\merge")
    val sortRdd = rdd.map(line=>{
      val split = line.split(",")
      (split(0),split(1).toInt)
    }).sortBy(_._2)
    sortRdd.saveAsTextFile("C:\\Users\\RUANWENJUN\\Desktop\\sortmerge")
  }

}
