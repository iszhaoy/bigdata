package com.iszhaoy.review

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, OutputTag, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * api demo
 *
 * @author zhaoyu
 * @date 2020/5/6
 */
object Exec04 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val fileDataStream = env.readTextFile(getClass.getClassLoader.getResource("sensor.txt").getPath)
      .map(x => {
        val dataArray: Array[String] = x.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
    // split > 32 切分
    val splitDataStream: SplitStream[SensorReading] = fileDataStream.split(x => {
      if (x.temperature > 32) Seq("hight") else Seq("low")
    })
    //    fileDataStream.print("fileDataStream")
    //    splitDataStream.select("hight").print("hight")
    //    splitDataStream.select("low").print("low")
    //    splitDataStream.select("hight", "low").print("all")

    // comap
    val connectedDataStream: ConnectedStreams[(String, Double), SensorReading] = splitDataStream.select("hight").map(data => (data.id, data.temperature)).connect(
      splitDataStream.select("low")
    )
    connectedDataStream.map(
      data2 => (data2._1, data2._2),
      data1 => (data1.id, data1.temperature, data1.timestamp)).print("connected")


//    splitDataStream.select("hight").union(splitDataStream.select("low")).print("all")

    env.execute("exec 04")
  }
}


