package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by fz on 2020/2/11
  */
object SparkStreaming04_KafkaSource {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val context: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(context, "hadoop102:2181", "atguigu", Map("atguigu" -> 3))

    //扁平化
    val wordsDStream: DStream[String] = kafkaDStream.flatMap(_._2.split(" "))

    //map
    val wordAndNumDStream: DStream[(String, Int)] = wordsDStream.map((_, 1))

    //聚合
    val resDStream: DStream[(String, Int)] = wordAndNumDStream.reduceByKey(_ + _)
    //将结果打印
    resDStream.print()

    //启动采集器
    context.start()

    //Driver等待采集器的执行
    context.awaitTermination()

  }

}
