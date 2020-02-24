package com.iszhaoy.source

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

object TestSensorSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从自定义的集合中读取 数据
    val stream1: DataStream[SensorReading] = env.addSource(new SensorSource())

    stream1.print("stream1").setParallelism(1)

//    stream1.keyBy(x => x.id).min(2).print().setParallelism(1)

    env.execute("TestSensorSource")

  }}
