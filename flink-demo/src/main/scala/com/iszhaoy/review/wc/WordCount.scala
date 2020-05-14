package com.iszhaoy.review.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @author zhaoyu
 * @date 2020/5/5
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    env.socketTextStream("localhost",9999)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print("wordcount")
    env.execute("wordcount")
  }
}
