package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by fz on 2020/2/7
  */
object Spark16_HBase {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDTest")

    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val cof: Configuration = HBaseConfiguration.create()

    cof.set(TableInputFormat.INPUT_TABLE, "rddtable")

    //从HBase中读取数据
    /*

    val HbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      cof,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    HbaseRDD.foreach {
      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (cell <- cells) {
          val value: String = Bytes.toString(CellUtil.cloneValue(cell))
          val colName: String = Bytes.toString(CellUtil.cloneQualifier(cell))
          val row: String = Bytes.toString(CellUtil.cloneRow(cell))
          println(s"主键为:${row},列名为:${colName},值:${value}")
        }
      }
    }

   */


    //向HBase中存储数据


    val putRDD = sc.makeRDD(List(("1003", "古天乐"), ("1004", "张家辉"), ("1005", "刘德华")))

    val putToHBase: RDD[(ImmutableBytesWritable, Put)] = putRDD.map {
      case (rowKey, name) => {
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
      }
    }

    val jobConf: JobConf = new JobConf(cof)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "rddtable")

    putToHBase.saveAsHadoopDataset(jobConf)



    //关闭资源
    sc.stop()

  }


}

