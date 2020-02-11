package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Create by fz on 2020/2/10
  */
object SparkSQL05_UDAF_class {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //进行转换时，需要引入隐式转换规则
    //这里的spark不是包名的含义，而是sparkSession对象的名字
    import spark.implicits._

    //创建rdd
//    val rdd: RDD[String] = sc.textFile("in/user.json")
//    rdd.foreach(println)

    val df: DataFrame = spark.read.json("in/user.json")
    val ds: Dataset[UserBean] = df.as[UserBean]


    val myAvg: MyAverageClass = new MyAverageClass
    val column: TypedColumn[UserBean, Double] = myAvg.toColumn.name("avgg")

    ds.select(column).show()

    spark.stop()

  }
}

case class UserBean(name: String, age: BigInt)

case class AvgAge(var ageSum: BigInt, var count: Int)

class MyAverageClass extends Aggregator[UserBean, AvgAge, Double] {
  //初始值
  override def zero: AvgAge = {
    AvgAge(0, 0)
  }

  //更新操作
  override def reduce(b: AvgAge, a: UserBean): AvgAge = {
    b.ageSum = b.ageSum + a.age
    b.count = b.count + 1
    b
  }

  //合并
  override def merge(b1: AvgAge, b2: AvgAge): AvgAge = {
    b1.ageSum = b1.ageSum + b2.ageSum
    b1.count = b1.count + b2.count
    b1
  }

  //返回
  override def finish(reduction: AvgAge): Double = {
    reduction.ageSum.toDouble / reduction.count
  }


  override def bufferEncoder: Encoder[AvgAge] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}