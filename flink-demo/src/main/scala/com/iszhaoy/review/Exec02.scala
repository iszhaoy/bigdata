package com.iszhaoy.review

import com.iszhaoy.apitest.{SensorReading, TimeIncreAlert, TimechangeAlert}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Flink api test
 *
 * @author zhaoyu
 * @date 2020/5/6
 */

object Exec02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val filterDataStream = env.socketTextStream("bigdata02", 9999)
      .map(x => {
        try {
          val dataArry: Array[String] = x.split(",")
          SensorReading(dataArry(0).trim, dataArry(1).trim.toLong, dataArry(2).trim.toDouble)
        } catch {
          case e: Exception => {
            println("data format error!")
          }
            SensorReading("", 0L, 0.0)
        }
      })
      .filter(_.id != "")
      .assignAscendingTimestamps(_.timestamp * 1000L)



    val processedStream: DataStream[(String, Double, Double)] = filterDataStream
      .keyBy(_.id)
      .process(new TimechangeAlert2(10.0))

    filterDataStream.print("filterDateStream")
    processedStream.print("processDataStream")

    env.execute("exec 02")

  }
}

class TimechangeAlert2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量，保存上次的温度值
  lazy val lasttempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",
    classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double,
    Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {

    // 获取上次的温度值
    val lastTemp: Double = lasttempState.value()

    // 用当前的温度之和上次的求差， 如果大于阈值就报警
    val diff: Double = (value.temperature - lastTemp).abs

    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lasttempState.update(value.temperature)
  }
}

class MyProcessFunction(threshold: Double) extends KeyedProcessFunction[String, SensorReading, String] {

  lazy val preTemState: ValueState[Double] = getRuntimeContext
    .getState(new ValueStateDescriptor[Double]("preTem", classOf[Double]))

  lazy val timeState: ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("timeState", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出上一次的温度值
    val preTem = preTemState.value()

    // 更新温度状态
    preTemState.update(value.temperature)

    // 温度连续上升且没有注册定时器，则注册定时器
    if (preTem < value.temperature && timeState.value() == 0) {
      // 获取当前的时间，设置定时器，延迟1秒
      val time = ctx.timerService().currentProcessingTime() + 10000L
      timeState.update(time)
      ctx.timerService().registerProcessingTimeTimer(time)
    } else if (preTem > value.temperature || preTem == 0) {
      // 取消定时器
      ctx.timerService().deleteProcessingTimeTimer(timeState.value())
      // 清空定时器状态
      timeState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    val sb = new StringBuilder
    sb.append(ctx.getCurrentKey).append("->")
      .append("温度上升")
    out.collect(sb.toString)
    // 报警完毕，清空状态
    timeState.clear()
  }
}



