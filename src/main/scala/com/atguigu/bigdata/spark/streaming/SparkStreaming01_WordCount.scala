package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by fz on 2020/2/11
  */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val context: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val dataDStream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102",9999)

    //扁平化
    val wordsDStream: DStream[String] = dataDStream.flatMap(_.split(" "))

    //map
    val wordAndNumDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //聚合
    val resDStream: DStream[(String, Int)] = wordAndNumDStream.reduceByKey(_+_)
    //将结果打印
    resDStream.print()

    //启动采集器
    context.start()

    //Driver等待采集器的执行
    context.awaitTermination()

  }

}
