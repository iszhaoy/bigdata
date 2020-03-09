package com.iszhaoy.windows

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streamFormFile: DataStream[String] = env.socketTextStream("bigdata01", 9999)
    val dataStream: DataStream[SensorReading] = streamFormFile.map(fun = data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp *1000
        }
      })

    // 处理时间反转窗口
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .reduce((d1, d2) => (d1._1 , d1._2.min(d2._2)))

    minTempPerWindowStream.print("mint temp")
    dataStream.print("input data")

    env.execute("windows")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 60000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000
  }
}


class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000
  }
}
