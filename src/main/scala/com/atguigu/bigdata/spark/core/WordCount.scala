package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/6
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //本地模式
    //创建spark配置对象，声明master-》local、name->wordCount
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //读取文件
    val lines: RDD[String] = sc.textFile("in")

    //扁平化操作，获取每个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将数据结构转化为k,v模式
    val wordToOne: RDD[(String, Int)] = words.map((_,1))

    //聚合操作
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey((_+_))

    val arr: Array[(String, Int)] = wordToSum.collect()

    arr.foreach(println)

  }
}
