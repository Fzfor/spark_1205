package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(1 to 10)

    //sample:从指定的数据集合中抽样处理，根据不同的算法进行抽样
    val r2: RDD[Int] = r1.sample(true,0.5,6)

    r2.collect().foreach(println)

  }

}
