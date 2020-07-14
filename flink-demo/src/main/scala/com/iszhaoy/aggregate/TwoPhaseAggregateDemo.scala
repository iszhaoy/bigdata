package com.iszhaoy.aggregate

import java.lang
import java.util.concurrent.ThreadLocalRandom

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/9 10:36
 * @version 1.0
 */

object TwoPhaseAggregateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path: String = getClass.getResource("/sensor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path)
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        SensorReading(
          dataArr(0).trim + "@" + ThreadLocalRandom.current().nextInt(3),
          dataArr(1).trim.toLong,
          dataArr(2).trim.toDouble
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
    dataStream.print("dataStream")

    val aggregateStreamForOne: DataStream[CountView] = dataStream
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .aggregate(new AggregateFuctionForOne(), new TimeWindowForOne())

    aggregateStreamForOne.print("one")

    val keyedStream: KeyedStream[CountView, (String, Long)] = aggregateStreamForOne
      .keyBy(data => (data.id, data.windowEnd))
    keyedStream.print("keyedStream")
    keyedStream
      .process(new CountKeyedProcessFunction())
      .print("two")

    env.execute("TwoPhaseAggregateDemo")

  }
}

class AggregateFuctionForOne() extends AggregateFunction[SensorReading, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: SensorReading, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class TimeWindowForOne() extends WindowFunction[Long, CountView, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountView]): Unit = {
    out.collect(CountView(key.split("@")(0), window.getEnd, input.iterator.next()))
  }
}

class CountKeyedProcessFunction() extends KeyedProcessFunction[(String, Long), CountView, (CountView)] {
  private var list: ListState[CountView] = _

  override def open(parameters: Configuration): Unit = {
    list = getRuntimeContext.getListState(new ListStateDescriptor[CountView]("countViewState", classOf[CountView]))
  }

  override def processElement(value: CountView, ctx: KeyedProcessFunction[(String, Long), CountView, CountView]#Context,
                              out: Collector[CountView]): Unit = {
    list.add(value)
    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, Long), CountView, CountView]#OnTimerContext,
                       out: Collector[CountView]): Unit = {
    import scala.collection.JavaConversions._
    var count:Long = 0L
    for(v <- list.get()) {
      count +=v.count
    }
    out.collect(CountView(ctx.getCurrentKey._1, timestamp - 1L,count))
  }
}