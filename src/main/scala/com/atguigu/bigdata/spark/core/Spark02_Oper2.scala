package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark02_Oper2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    //从内存中创建RDD对象，makeRDD，底层实现就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartitions可以对一个RDD中的所有分区进行遍历
    //mapPartitions效率高于map，减少了发送到执行器执行excutor的交互次数
    //mapPartitions可能会造成内存溢出（OOM）
    val resRDD: RDD[Int] = listRDD.mapPartitions(datas =>
      datas.map(_ * 2)
    )

    resRDD.collect().foreach(println)
  }

}
