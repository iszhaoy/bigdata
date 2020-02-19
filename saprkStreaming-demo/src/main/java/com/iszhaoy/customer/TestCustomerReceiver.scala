package com.iszhaoy.customer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestCustomerReceiver {
  def main(args: Array[String]): Unit = {
    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 3. 创建DStream
    val inputStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("bigdata01",9999))

    inputStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
