package com.iszhaoy.kafka

import java.util.Properties

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamFormFile: DataStream[String] = env.readTextFile("flink-demo\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[String] = streamFormFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        // kafka需要String
        .toString
    })

    // 二阶段提交事务 保证端到端一致性
    val outprop: Properties = new Properties()
    outprop.setProperty("bootstrap.servers", "hadoop01:9092")
    //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
    //设置事务超时时间
    outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    val kafkaProducer = new FlinkKafkaProducer011[String](
      "outputTopic",
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema),
      outprop,Semantic.EXACTLY_ONCE)
    kafkaProducer.setWriteTimestampToKafka(true)
    dataStream.addSink(kafkaProducer)
  }
}
