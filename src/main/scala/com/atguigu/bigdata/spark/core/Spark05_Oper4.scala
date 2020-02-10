package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Array[Int]] = sc.makeRDD(List(Array(1,2),Array(4,5)))

    //flatMap
    val r2: RDD[Int] = r1.flatMap(datas=>datas)
    r2.collect().foreach(println)
  }

}
