package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    //从内存中创建RDD对象，makeRDD，底层实现就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)


    val indexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      (num, datas) => {
        datas.map((_, "分区号是：" + num))
      }
    }

    indexRDD.collect().foreach(println)
  }

}
