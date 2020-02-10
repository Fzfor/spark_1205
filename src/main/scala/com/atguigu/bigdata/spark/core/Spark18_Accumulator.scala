package com.atguigu.bigdata.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark18_Accumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val r1: RDD[String] = sc.makeRDD(Array("hadoop","scala","spark","hbase","hive"), 2)

    //val accumulator: LongAccumulator = sc.longAccumulator

    //TODO 创建自定义的accumulator
    val accumulator = new WordAccumulator()
    //TODO 向spark注册accumulator
    sc.register(accumulator)



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


class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()
  //初始值
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    this
  }

  //重置
  override def reset(): Unit = {
    list.clear()
  }

  //添加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //返回值
  override def value: util.ArrayList[String] = {
    list
  }
}
