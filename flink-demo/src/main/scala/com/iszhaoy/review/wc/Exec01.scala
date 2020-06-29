package com.iszhaoy.review.wc

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 时间窗口，判断窗口内出现的最小温度
 *
 * @author zhaoyu
 * @date 2020/5/5
 */

object Exec01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val socketDataStream: DataStream[String] = env.socketTextStream("bigdata02", 9999)

    val dataStream: DataStream[SensorReading] = socketDataStream.map(x => {
      try {
        val dataArry: Array[String] = x.split(",")
        SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
      } catch {
        case e: Exception => println("data format error")
          SensorReading("", 0L, 0.0)
      }
    })
      .filter(_.id != null)
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
      //        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      //      })
      .assignTimestampsAndWatermarks(new MyAssigner())
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(x => (x.id, x.temperature))
      .keyBy(_._1)
      //            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .reduce((x, y) => (x._1, x._2.min(y._2)))
    minTempPerWindowStream.print("exec 01")
    env.execute("exec 01")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 1000L
  var maxTime = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTime - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTime = maxTime.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}
