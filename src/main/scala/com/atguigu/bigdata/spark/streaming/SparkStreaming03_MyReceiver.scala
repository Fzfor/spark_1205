package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by fz on 2020/2/11
  */
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val context: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val receiverDStream: ReceiverInputDStream[String] = context.receiverStream(new MyReceiver("hadoop102",9999))

    //扁平化
    val wordsDStream: DStream[String] = receiverDStream.flatMap(_.split(" "))

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

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket: Socket = null

  def receive(): Any = {
    socket = new Socket(host, port)

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,"UTF-8"))

    var line: String = null
    while ((line = reader.readLine()) != null) {
      if ("END".equals(line)) {
        return
      }else{
        this.store(line)
      }

    }

  }

  override def onStart(): Unit = {

      new Thread(new Runnable {
        override def run(): Unit = {
          receive()
        }
      }).start()

  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
