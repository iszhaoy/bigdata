package com.iszhaoy.review

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/29 11:24
 * @version 1.0
 */
object KafkaStreamUpdateStatusByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/ck-2020-06-29")
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

    //   updateFunc: (Seq[V], Option[S]) => Option[S]
    val func = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum
      val prevCount: Int = state.getOrElse(0)
      Some(currentCount + prevCount)
    }

    val resDStream: DStream[(String, Int)] = topicDStream.flatMap(_._2.split("\\s+"))
      .map((_, 1))
      .updateStateByKey[Int](func)

    resDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
