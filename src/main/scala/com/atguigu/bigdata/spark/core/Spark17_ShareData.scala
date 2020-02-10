package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark17_ShareData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val r1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val accumulator: LongAccumulator = sc.longAccumulator

    r1.foreach {
      case i => {
        accumulator.add(i)
      }
    }

    println(s"和为：${accumulator.value}")


    //关闭资源
    sc.stop()

  }


}

