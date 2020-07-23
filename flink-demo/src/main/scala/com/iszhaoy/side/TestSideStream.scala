package com.iszhaoy.side

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TestSideStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streamFormFile: DataStream[String] = env.socketTextStream("hadoop01", 9999)
    val dataStream: DataStream[SensorReading] = streamFormFile.map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    val sideOutPut: OutputTag[(String,Double)] = new OutputTag[(String,Double)]("side_output")
    //处理时间反转窗口
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // 当指定允许的延迟大于0时，在水印通过窗口结束后保持窗口及其内容。在这些情况下，当迟到但未掉落的数据元到达时，它可能触发窗口的另一次触发。这些触发
      // 被称为 late firings ，因为它们是由迟到事件触发的，与之相反的 main firing 是窗口的第一次触发。
      // 后期触发发出的数据元应该被视为先前计算的更新结果，即数据流将包含同一计算的多个结果。在应用程序中需要考虑这些重复的结果或对其进行重复数据删除
      .allowedLateness(Time.seconds(5))
      .sideOutputLateData(sideOutPut)
      .reduce((d1, d2) => (d1._1, d1._2.min(d2._2)))

    minTempPerWindowStream.getSideOutput(sideOutPut).print("last")

    minTempPerWindowStream.print("mint temp")
    dataStream.print("input data")

    env.execute("TestSideStream")
  }
}
