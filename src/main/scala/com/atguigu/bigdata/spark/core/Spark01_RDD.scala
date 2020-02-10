package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    //从内存中创建RDD对象，makeRDD，底层实现就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    listRDD.collect().foreach(println)

    //从内存中创建RDD对象，parallelize，底层实现就是parallelize
//    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))
//    arrayRDD.collect().foreach(println)

    //从外部存储中创建RDD对象
    //默认可以读取项目的路径，也可以读取其他路径，如：HDFS
    //默认从文件中读取的数据都是String类型
    //去读文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时分片规则
//    val fileRDD: RDD[String] = sc.textFile("in")

    //将RDD中的数据保存到文件中
    listRDD.saveAsTextFile("out")


  }

}
