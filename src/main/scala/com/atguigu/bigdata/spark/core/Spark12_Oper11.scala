package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    val r1: RDD[(Int, String)] = sc.makeRDD(List((1, "James"), (2, "D"), (3, "littlesTU"), (4, "KD")),4)

    val r2: RDD[(Int, String)] = r1.partitionBy(new MyPartioner(3))

    r2.saveAsTextFile("out")

  }


}

class MyPartioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
    3
  }

  override def getPartition(key: Any): Int = {
    2
  }
}