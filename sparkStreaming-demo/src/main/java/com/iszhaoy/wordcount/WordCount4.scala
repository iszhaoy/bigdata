package com.iszhaoy.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount4 {
  def main(args: Array[String]): Unit = {

    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.checkpoint("./ck2")

    // 3. 通过监听端口穿件Dstream 读进来的数据为一行
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)

    // 切分
    val wordToOne: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_, 1))

    // 聚合4个批次的数组（12秒）  每个3秒计算

    val result: DStream[(String, Int)] = wordToOne
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y,
        Seconds(6),
        Seconds(2),
        4,
        // 过滤条件
        (x: (String, Int)) => x._2 > 0
      )

    //打印
    result.print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
