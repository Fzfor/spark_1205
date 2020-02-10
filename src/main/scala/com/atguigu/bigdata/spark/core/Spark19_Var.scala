package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark19_Var {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val r1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 2), (2, 3), (3, 4), (8, 8)), 2)

    //val r2: RDD[(Int, Int)] = sc.makeRDD(Array((1,8),(2,8),(3,8),(8,8)), 2)

    //val r3: RDD[(Int, (Int, Int))] = r1.join(r2)
    // r3.foreach(println)

    //join设计笛卡尔积，效率低，可以使用广播变量优化
    val list: List[(Int, Int)] = List((1, 8), (2, 8), (3, 8), (8, 8))

    //TODO 创建广播变量
    val broad: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    val r3: RDD[(Int, (Int, Any))] = r1.map {
      case (x, y) => {
        var v2: Any = null
        for (item <- broad.value) {
          if (item._1 == x) {
            v2 = item._2
          }
        }
        (x, (y, v2))
      }
    }
    r3.foreach(println)



    //关闭资源
    sc.stop()

  }


}

