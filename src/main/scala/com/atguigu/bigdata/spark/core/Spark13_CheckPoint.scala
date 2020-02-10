package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark13_CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("CheckPoint")

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(List(1,2,3,1,6,7,5,7,8,8,8))

    val r2: RDD[(Int, Int)] = r1.map((_,1))


    val r3: RDD[(Int, Int)] = r2.reduceByKey(_+_)
    r3.checkpoint()

    r3.foreach(println)

    println(r3.toDebugString)

    sc.stop()

  }


}

