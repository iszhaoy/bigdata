package com.iszhaoy.trigger

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/8/18 14:20
 * @version 1.0
 *
 *          利用flink窗口机制去重,针对有主键的数据
 */
object TriggerDemo2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(1)
    val streamFormFile: DataStream[String] = env.socketTextStream("localhost", 9999)
    val dataStream: DataStream[SensorReading] = streamFormFile.map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
    dataStream
      .keyBy(_.id)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .trigger(new DuplicationTrigger(Time.seconds(10).toMilliseconds))
      .reduce((s1, s2) => s1)
      .print("test")

    env.execute("test")
  }
}

class DuplicationTrigger(interal: Long) extends Trigger[SensorReading, TimeWindow] {

  override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
    // firstSeen will be false if not set yet
    // 为什么这里的状态默认不是false??????????????
    val firstSeen: ValueState[Boolean] =
      ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",
          classOf[Boolean]))
    println(firstSeen.value())
    if (!firstSeen.value()) {
      //if (window.getEnd - window.getStart == interal) {
      // 如果窗口大小和session gap大小相同，则判断为第一条数据；这是因为第二条消息到达后，窗口Merge会导致窗口变大。
      firstSeen.update(true)
      TriggerResult.FIRE_AND_PURGE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {}

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {}
}

