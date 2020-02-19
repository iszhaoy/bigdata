package com.iszhaoy.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCoun3 {
  def main(args: Array[String]): Unit = {

    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./ck2")


    // 3. 通过监听端口穿件Dstream 读进来的数据为一行
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)

    // 切分
    val wordToOne: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_, 1))

    // 统计
    val result: DStream[(String, Int)] = wordToOne.updateStateByKey(updateFunc)

    //打印
    result.print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
