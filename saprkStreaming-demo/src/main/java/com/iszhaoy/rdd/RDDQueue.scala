package com.iszhaoy.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueue {
  def main(args: Array[String]): Unit = {
    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

   //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)

    //5.处理队列中的RDD数据
    val reducedStream = inputStream.reduce(_ + _)

    reducedStream.print()

    ssc.start()

    //8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()

  }
}
