package com.atguigu.bigdata.spark.core

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark15_MySQL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "zb346224813"

    val sql1 = "select * from user where id >= ? and id <= ?"

    val r1 = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        java.sql.DriverManager.getConnection(url, userName, passWd)
      },
      sql1,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString(2) + "," + rs.getInt(3))
      }
    )

    //r1.collect()


    val players: RDD[(String, Int)] = sc.parallelize(List(("Jamse", 23), ("KD", 35), ("Kobe", 24)))

    /**
      * 这种办法创建了过多的con对象，效率太低，不采取
      *players.foreach{
      * case (name, num) => {
      * val con: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      * *
      * var sql2 = "insert into user (name,age) values (?,?)"
      * *
      * val statement: PreparedStatement = con.prepareStatement(sql2)
      * *
      *statement.setString(1,name)
      *statement.setInt(2,num)
      * *
      *statement.execute()
      * *
      *statement.close()
      *con.close()
      * *
      *
      * }
      *
      */

    players.foreachPartition (

      datas => {
        val con: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
        datas.foreach {
          case (name, num) => {
            var sql2 = "insert into user (name,age) values (?,?)"
            val statement: PreparedStatement = con.prepareStatement(sql2)
            statement.setString(1, name)
            statement.setInt(2, num)

            statement.executeUpdate()

            statement.close()
          }
        }
        con.close()
      }
  )

    sc.stop()

  }


}

