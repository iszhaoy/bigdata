package com.iszhaoy.wc.com.iszhaoy.sink

import com.iszhaoy.wc.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object TestSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamFormFile: DataStream[String] = env.readTextFile("flink-demo\\src\\main\\resources\\sernsor.txt")

    val dataStream: DataStream[String] = streamFormFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        // kafka需要String
        .toString
    })

    // sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092",
      "test", new SimpleStringSchema()))
    dataStream.print()
  }
}
