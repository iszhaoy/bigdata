package com.iszhaoy.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

// 传感器读数样例类

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从自定义的集合中读取 数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    stream1.print("stream1").setParallelism(6)

    // 2 从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("flink-demo\\src\\main\\resources\\sernsor.txt")
    stream2.print("stream2").setParallelism(1)

    // 3. 从不同的元素中读取
    env.fromElements(1, 2, 3, "hello").print("stream3")


    // 4.从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 连接Kafka
    val stream4: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String](
        "sensor",
        new SimpleStringSchema(),
        properties))
    stream4.print("stream4").setParallelism(1)

    env.execute("SourceTest")
  }
}
