package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark11_Oper10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(1 to 16 ,4)
    r1.mapPartitionsWithIndex{
      (no,datas)=>{
        datas.map(_+"的分区号是："+no)
      }
    }.collect().foreach(println)

    println("*********")

    //缩减分区数：可以简单理解为合并分区
    val r2: RDD[Int] = r1.coalesce(3)
    r2.mapPartitionsWithIndex{
      (no,datas)=>{
        datas.map(_+"的分区号是："+no)
      }
    }.collect().foreach(println)
  }

}
