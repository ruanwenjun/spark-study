package com.wenjun.scalastudy

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将SparkRdd中数据从一对多映射转为一对一映射
  */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val list = new scala.collection.mutable.ListBuffer[String]()
    list += "小明,戏剧|爱情|动画"
    list += "小红,戏剧|爱情|动画"
    list += "小智,戏剧|爱情|动画|魔法|综艺"
    val rdd = sc.parallelize(list)
    val interestRdd = rdd.map(man => {
      val split = man.split(",")
      (split(0), split(1).split("\\|"))
    })
    val resRdd = interestRdd.flatMap(tuple => {
      for (i <- tuple._2) yield (i, tuple._1)
    }).groupByKey()
    resRdd.foreach(println)
  }

}
