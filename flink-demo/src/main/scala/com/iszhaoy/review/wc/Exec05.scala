package com.iszhaoy.review.wc

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

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
      ("Li", "Math", 87),
      ("Zhang", "Math", 62),
      ("Zhang", "English", 92),
      ("Yang", "Math", 90),
      ("Li", "English", 73)
    ).keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, String, Int), String] {
        lazy val sum: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("sum", classOf[Int]))

        override def processElement(value: (String, String, Int), ctx: KeyedProcessFunction[String, (String, String, Int), String]#Context, out: Collector[String]): Unit = {
          sum.update(sum.value() + value._3)
          val value1 = s"${value._1},sum,${sum.value}"
          out.collect(value1)
        }
      }).print()
    env.execute("exec 05")
  }
}
