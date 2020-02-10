package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/8
  */
object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AD")

    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("F:\\datasForPractice", 7)

    //val unit: Unit = data.saveAsTextFile("out")

    //将数据转换为(省份+广告,1)的形式
    //    val provinceAndAD: RDD[(String, Int)] = data.mapPartitions {
    //      lines => {
    //        lines.map {
    //          line => {
    //            val words: Array[String] = line.split(" ")
    //            ((words(1) + "+" + words(4)), 1)
    //          }
    //        }
    //      }
    //    }

    val provinceAndAD: RDD[((String, String), Int)] = data.mapPartitions {
      lines => {
        lines.map {
          line => {
            val words: Array[String] = line.split(" ")
            ((words(1), words(4)), 1)
          }
        }
      }
    }
    //provinceAndAD.collect().foreach(println)

    //求出每个省每个广告的总的点击量
    val total: RDD[((String, String), Int)] = provinceAndAD.reduceByKey(_ + _)

    //按照省份分组，将省份字段设为k,并分组
    val provinceToK: RDD[(String, Iterable[(String, Int)])] = total.map(x => (x._1._1, (x._1._2, x._2))).groupByKey()

    provinceToK.collect().foreach(println)

    val res: RDD[(String, List[(String, Int)])] = provinceToK.mapValues {
      x => {
        x.toList.sortWith {
          (x, y) => {
            x._2 > y._2
          }
        }.take(3)
      }
    }
    res.collect().foreach(println)
    //provinceWithSort.collect().foreach(println)


    //按照

    //total.collect().foreach(println)

    sc.stop()

  }
}
