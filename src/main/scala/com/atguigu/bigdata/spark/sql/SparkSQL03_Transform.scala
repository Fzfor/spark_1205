package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by fz on 2020/2/10
  */
object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //进行转换时，需要引入隐式转换规则
    //这里的spark不是包名的含义，而是sparkSession对象的名字
    import spark.implicits._

    //创建rdd
    val rdd: RDD[(Int, String, Int)] = sc.makeRDD(List((1, "James", 23), (2, "KD", 35), (3, "Kobe", 24)))

    //1.装换为df
    val df1: DataFrame = rdd.toDF("id","name","num")
    //df1.show()

    //2.转换为ds
    //2.1df->ds
    val ds1: Dataset[User] = df1.as[User]
    //ds1.show()

    //2.2rdd->ds
    val rddWithUser: RDD[User] = rdd.map {
      case x => {
        User(x._1, x._2, x._3)
      }
    }
    val ds2: Dataset[User] = rddWithUser.toDS()
    //ds2.show()

    //3.ds转换为df
    val df2: DataFrame = ds1.toDF()
    //df2.show()


    //4.转换为rdd
    //4.1 ds->rdd
    val rdd1: RDD[User] = ds1.rdd
    //rdd1.foreach(user=>{println(user.name)})

    //4.2 df -> rdd
    val rdd2: RDD[Row] = df1.rdd
    rdd2.foreach(user=>{println(user.getString(1))})

    spark.stop()

  }
}

case class User(id: Int, name: String, num: Int)
