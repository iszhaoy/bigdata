package com.iszhaoy.aggregate

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/9 10:14
 * @version 1.0
 */

case class CountView(id: String, windowEnd: Long, count: Long)

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path: String = getClass.getResource("/sensor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path)
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
      })
      //.assignTimestampsAndWatermarks()
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val aggregateStream: DataStream[CountView] = dataStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .aggregate(new AggregateFunction[SensorReading, Long, Long]() {
        override def createAccumulator() = 0L

        override def add(value: SensorReading, accumulator: Long) = accumulator + 1L

        override def getResult(accumulator: Long) = accumulator

        override def merge(a: Long, b: Long) = a + b
      }, new WindowFunction[Long, CountView, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountView]): Unit = {
          out.collect(CountView(key, window.getEnd, input.iterator.next()))
        }
      })

    aggregateStream.print("agg")
    env.execute("AggregateDemo")

  }
}
