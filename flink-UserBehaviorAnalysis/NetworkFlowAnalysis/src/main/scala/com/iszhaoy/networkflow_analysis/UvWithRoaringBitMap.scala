package com.iszhaoy.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.roaringbitmap.RoaringBitmap

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/11 16:00
 * @version 1.0
 */


object UvWithRoaringBitMap {
  def main(args: Array[String]): Unit = {
    //val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(3)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim,
          dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(x => ("dumb", x.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new RoaringBitMapAggregateFunction(), new RoaringBitMapWindowFunction()).setParallelism(6)
    dataStream.print("UniqueWithRoaringBitMap")
    env.execute("UniqueWithRoaringBitMap")
  }
}

class RoaringBitMapAggregateFunction() extends AggregateFunction[(String, Long), RoaringBitmap, Long] {
  override def createAccumulator(): RoaringBitmap = new RoaringBitmap()

  override def add(value: (String, Long), accumulator: RoaringBitmap): RoaringBitmap = {
    accumulator.add(value._2.hashCode())
    accumulator
  }

  override def getResult(accumulator: RoaringBitmap): Long = {
    accumulator.getLongCardinality
  }

  override def merge(a: RoaringBitmap, b: RoaringBitmap): RoaringBitmap = {
    a.or(b)
    a
  }
}

class RoaringBitMapWindowFunction() extends WindowFunction[Long,UvCount,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd,input.iterator.next()))
  }
}

