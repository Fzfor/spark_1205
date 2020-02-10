package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(Array(2,3,5,5,5,2,5,6,8,7,7,8,2))

    println("去重前：")
    r1.mapPartitionsWithIndex{
      (no,datas)=>{
        datas.map((_,"分区号是："+no))
      }
    }.collect().foreach(println)

    //distinct:去重
    //val r2: RDD[Int] = r1.distinct()

    //去重后数据减少，所以可以加参数来减少分区数(改变默认分区数量)来存储数据
    val r2: RDD[Int] = r1.distinct(3)
    println("去重后：")

    r2.mapPartitionsWithIndex{
      (no,datas)=>{
        datas.map(_+"的分区号是："+no)
      }
    }.collect().foreach(println)

    //r2.collect().foreach(println)

    r2.saveAsTextFile("out")


  }

}
