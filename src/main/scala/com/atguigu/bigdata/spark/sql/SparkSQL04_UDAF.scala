package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by fz on 2020/2/10
  */
object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //进行转换时，需要引入隐式转换规则
    //这里的spark不是包名的含义，而是sparkSession对象的名字
    import spark.implicits._

    //创建rdd
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(List((1, "James", 23), (2, "KD", 35), (3, "Kobe", 24)))

    val df: DataFrame = rdd.toDF("id","name","age")

    val players: Unit = df.createOrReplaceTempView("players")

    //注册自己写的聚合函数
    val myAverage = new MyAverage
    spark.udf.register("avgAge",myAverage)

    spark.sql("select avgAge(age) from players").show()



    spark.stop()

  }
}

class MyAverage extends UserDefinedAggregateFunction {
  //输入的类型
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //缓冲区的类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //输出的类型
  override def dataType: DataType = {
    DoubleType
  }

  //函数稳定性，输出同一个参数，得到的结果是否一样
  override def deterministic: Boolean = true

  //缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //返回
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}