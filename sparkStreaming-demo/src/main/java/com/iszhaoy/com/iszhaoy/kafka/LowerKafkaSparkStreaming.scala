package com.iszhaoy.com.iszhaoy.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object LowerKafkaSparkStreaming {


  def getOffset(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {
    // 定义一个最终返回类，主题分区 -》offset
    var partitionToLong = new HashMap[TopicAndPartition, Long]()

    // 根据指定的topic获取分区
    val topicAndPartitionsEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    // 判断分区是否存在
    if (topicAndPartitionsEither.isRight) {
      // 分区信息不为空 取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionsEither.right.get

      // 获取消费者消费的进度
      val topicAndPartitionsToLongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      // 判断offset是否存在
      if (topicAndPartitionsToLongEither.isLeft) {

        // offset不存在，（该消费者为消费过）,遍历每一个分区
        for (topicAndPartition <- topicAndPartitions) {
          // 使用SimpleConsumer获取该分区的最小值
          partitionToLong += (topicAndPartition -> 0)
        }
      } else {
        // 取出offset
        val value: Map[TopicAndPartition, Long] = topicAndPartitionsToLongEither.right.get
        partitionToLong ++= value
      }

    }
    partitionToLong
  }


  def setOffset(kafkaCluster: KafkaCluster, kafkaDStream: InputDStream[String], group: String) = {

    kafkaDStream.foreachRDD(rdd => {

      var partitionToLong = new HashMap[TopicAndPartition, Long]()
      // 取出RDD的offset
      val offsetRangs: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      // 获取所有分区的osffetRange
      val ranges: Array[OffsetRange] = offsetRangs.offsetRanges

      // 遍历数组
      for (range <- ranges) {
        partitionToLong += (range.topicAndPartition() -> range.untilOffset)
        // 对每个分区提交一次
        // kafkaCluster.setConsumerOffsets(group,Map(range.topicAndPartition()-> range.untilOffset))
      }
      // 对所有分区提交
      kafkaCluster.setConsumerOffsets(group, partitionToLong)
    })

  }

  def main(args: Array[String]): Unit = {
    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafkaSparkStreaming")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // kafka参数
    val borkers: String = "bigdata01:9092,bigdata02:9092,bigdata03:9092"
    val topic: String = "second"
    val group: String = "iszhaoy"
    val deserializationClass: String = "org.apache.kafka.common.serialization.StringSerializer"

    val params: Map[String, String] = Map[String, String](
      "zookeeper.connect" -> "bigdata01:2181,bigdata02:2181,bigdata03:2181",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> borkers
    )

    val kafkaCluster = new KafkaCluster(params)

    val fromOffset: Map[TopicAndPartition, Long] = getOffset(kafkaCluster, group, topic)

    // 读取kafka中的数据
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      params,
      fromOffset,
      (message:MessageAndMetadata[String,String]) => message.message()
    )

    // 打印
    kafkaDStream.print()


    // 保存Offset
    setOffset(kafkaCluster, kafkaDStream, group)


    ssc.start()
    ssc.awaitTermination()

  }


}
