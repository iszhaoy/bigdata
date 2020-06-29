package com.iszhaoy.review

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/29 11:24
 * @version 1.0
 */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val broker_list = "hadoop01:9092"
    val topic = "topic-2020-06-29"
    val group_id = "iszhaoy001"

    val kafkaParam = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id
    )

    val topicDStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(topic))
    //    topicDStream
    //      .flatMap(x => x._2.split("\\s+"))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .foreachRDD(rdd => {
    //        rdd.foreach(println)
    //      })

    //    topicDStream.foreachRDD(rdd => {
    //      rdd.flatMap(_._2.split("\\s+"))
    //        .map((_,1))
    //        .reduceByKey(_+_)
    //        .foreachPartition(iter => {
    //          while(iter.hasNext) {
    //            println(iter.next())
    //          }
    //        })
    //    })
    topicDStream.transform(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordDF: DataFrame = rdd.flatMap(_._2.split("\\s+"))
        .toDF("word")
      val wcDF: DataFrame = wordDF.groupBy($"word").count()
      wcDF.rdd
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
