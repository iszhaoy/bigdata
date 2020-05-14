package com.iszhaoy.review.wc

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * 时间窗口，判断窗口内出现的最小温度
 *
 * @author zhaoyu
 * @date 2020/5/5
 */

object Exec05 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.fromElements(
      ("Yang", "English", 95),
      ("Zhang", "Math", 62),
      ("Li", "Math", 87),
      ("Zhang", "English", 92),
      ("Yang", "Math", 90)
    ).keyBy(_._1)
      // 全局窗口，会将所有的数据放在一个窗口中，需要自己制定触发器，
      .window(GlobalWindows.create())
      .trigger(new MyTrigger(2))
      .process(new ProcessWindowFunction[(String, String, Int), (String, String, Int), String, GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, String, Int)], out: Collector[
          (String, String, Int)]): Unit = {
          var sum = 0
          for (elem <- elements) {
            sum += elem._3
          }
          out.collect((key, "sum", sum))
        }
      })
      .print("test")

    env.execute("exec05")
  }
}

class MyTrigger2(num: Int) extends Trigger[(String, String, Int), GlobalWindow] {
  var flag = 0

  override def onElement(element: (String, String, Int), timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext)
  : TriggerResult = {
    if (flag == num) {
      flag = 0
      println(s"fire     ===============       $flag     $element")
      TriggerResult.FIRE_AND_PURGE
    } else {
      flag += 1
      println(s"continue ===============       $flag     $element")
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult
    .CONTINUE

  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {}
}


class MyTrigger(num: Int) extends Trigger[(String, String, Int), GlobalWindow] {
  // 使用map保存用户的课程数量
  val map = new mutable.HashMap[String, Int]()

  override def onElement(element: (String, String, Int), timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext)
  : TriggerResult = {
    if (map.getOrElseUpdate(element._1, 0) + 1 == num) {
      // 说明用户的课程都已经来了，删除这个用户的数据，节约资源
      map.remove(element._1)
      println(s"fire===============       $map")
      TriggerResult.FIRE_AND_PURGE
    } else {
      map.put(element._1, map.getOrElseUpdate(element._1, 0) + 1)
      println(s"continue===============       $map")
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult
    .CONTINUE

  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {}
}
