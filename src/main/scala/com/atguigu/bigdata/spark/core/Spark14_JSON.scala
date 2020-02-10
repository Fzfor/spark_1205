package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Create by fz on 2020/2/7
  */
object Spark14_JSON {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val r1: RDD[String] = sc.textFile("in/user.json")

    val r2: RDD[Option[Any]] = r1.map(JSON.parseFull)

    r2.foreach(println)

    sc.stop()

  }


}

