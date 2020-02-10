package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Create by fz on 2020/2/10
  */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("in/user.json")

    df.createOrReplaceTempView("user")

    val frame: DataFrame = spark.sql("select name from user")

    frame.show()

    spark.stop()

  }
}
