package com.iszhaoy.com.iszhaoy.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HighKafkaSparkStreaming2 {

  def ccreateSSC(): StreamingContext = {
    //2.初始化SparkStreamingContext
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafkaSparkStreaming")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck1")
    // kafka参数
    val borkers: String = "bigdata01:9092,bigdata02:9092,bigdata03:9092"
    val topic: String = "first"
    val group: String = "iszhaoy"
    val deserializationClass: String = "org.apache.kafka.common.serialization.StringSerializer"

    val params: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> borkers
    )

    // 读取kafka中的数据
    val value: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, Set(topic))

    value.map(_._2).print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    // 初始spark配置信息
    val ssc = StreamingContext.getActiveOrCreate("./ck1",()=>ccreateSSC())
    // 启动
    ssc.start()
    ssc.awaitTermination()

  }
}
