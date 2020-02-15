package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark20_MyClassSerializable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val r1: RDD[String] = sc.makeRDD(Array("hello", "hbase", "hello", "hadoop", "spark"))

    val search: Search = new Search("a")

    val r2: RDD[String] = search.getMatch2(r1)

    r2.foreach(println)

    //关闭资源
    sc.stop()

  }


}

class Search(query: String) extends Serializable {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch(_))
  }

  def getMatch2(rDD: RDD[String]): RDD[String] = {
    rDD.filter(x => x.contains(query))
  }

}

