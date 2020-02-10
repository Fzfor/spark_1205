package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //创建RDD对象
    //从内存中创建RDD对象，makeRDD，底层实现就是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //Map算子
    //所有算子中的计算都由Executor完成



    //val resRDD: RDD[Int] = listRDD.map(_ * 2)
    val resRDD: RDD[Unit] = listRDD.map(new User().setAge(_))


    resRDD.collect().foreach(println)
  }

}

class User{
  var age = 0
  def setAge(sage:Int):Unit={
    this.age = sage
  }
}
