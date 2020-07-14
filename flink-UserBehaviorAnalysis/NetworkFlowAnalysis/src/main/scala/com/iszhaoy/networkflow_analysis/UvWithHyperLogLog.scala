package com.iszhaoy.networkflow_analysis

import net.agkn.hll.HLL
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/11 16:00
 * @version 1.0
 */


object UvWithHyperLogLog {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
        .map(x => ("dumb",x.userId))
        .keyBy(_._1)
        .timeWindow(Time.hours(1))
        .aggregate(new HyperLogLogAggregateFunction(),new HyperLogLogWindowFunction())
    dataStream.print("UniqueWithHyperLogLog")
    env.execute("UniqueWithHyperLogLog")

  }
}

class HyperLogLogAggregateFunction() extends AggregateFunction[(String,Long),HLL,Long] {
  override def createAccumulator(): HLL = new HLL(14,6)

  override def add(value: (String, Long), accumulator: HLL): HLL = {
    accumulator.addRaw(value._2)
    accumulator
  }

  override def getResult(accumulator: HLL): Long = {
    accumulator.cardinality()
  }

  override def merge(a: HLL, b: HLL): HLL = {
    a.union(b)
    a
  }
}

 class HyperLogLogWindowFunction() extends WindowFunction[Long,UvCount,String,TimeWindow] {
   override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
     out.collect(UvCount(window.getEnd,input.iterator.next()))
   }
 }

