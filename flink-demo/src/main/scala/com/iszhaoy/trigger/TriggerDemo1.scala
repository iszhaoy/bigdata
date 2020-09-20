package com.iszhaoy.trigger

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/8/18 9:47
 * @version 1.0
 */
object TriggerDemo1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streamFormFile: DataStream[String] = env.socketTextStream("localhost", 9999)
    val dataStream: DataStream[SensorReading] = streamFormFile.map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(0)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    dataStream.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new Trigger[SensorReading, TimeWindow] {
        //new ValueStateDescriptor[Boolean]("isFrist",classOf[Boolean])
        //private lazy val isFirst:ValueState[Boolean] =
        override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: TriggerContext) = {
          val interval: Long = Time.seconds(2).toMilliseconds

          // 使用getWindowStartWithOffset获取timer时间
          val timer: Long = TimeWindow.getWindowStartWithOffset(ctx.getCurrentWatermark, 0, interval) + interval
          ctx.registerEventTimeTimer(timer)
          println(s" window start ${window.getStart} end ${window.getEnd} watermark ${ctx.getCurrentWatermark} timer ${timer}")
          TriggerResult.CONTINUE
        }
        override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext) = {

          TriggerResult.CONTINUE
        }
        override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext) = {
          println(s" timer fire $time")
          if (time < 0 || ctx.getCurrentWatermark >= time) {
            TriggerResult.CONTINUE
          } else {
            // 窗口数据触发后需要保留
            TriggerResult.FIRE
          }
        }
        override def clear(window: TimeWindow, ctx: TriggerContext) = {
        }
      })
      .reduce((s1, s2) => {
        SensorReading(s1.id, s2.timestamp, s1.temperature + s2.temperature)
      })
      .print("test")

    env.execute("trigger")
  }
}
