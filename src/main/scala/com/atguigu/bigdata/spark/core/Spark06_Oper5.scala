package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark06_Oper5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(1 to 10,4)

    //glom:将每个分区中的数据放到一个数组中
    val r2: RDD[Array[Int]] = r1.glom()
    r2.collect().foreach(array=>println(array.mkString(",")))
  }

}
