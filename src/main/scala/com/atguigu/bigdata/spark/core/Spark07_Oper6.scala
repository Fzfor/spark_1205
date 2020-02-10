package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark07_Oper6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[Int] = sc.makeRDD(1 to 9)

    //groupBy，按照指定的规则进行分组
    //分组后的数据形成对欧元组（k,v）,k表示分组的key，v表示组内数据集合
    val r2: RDD[(Int, Iterable[Int])] = r1.groupBy(a=>a%2)

    //查看分组后的数据的分区情况
    val r3: RDD[(Int, Iterable[Int], String)] = r2.mapPartitionsWithIndex {
      (num, datas) => {
        datas.map(tuple => (tuple._1, tuple._2, "分区号是：" + num))
      }
    }

    r3.collect().foreach(println)


  }

}
