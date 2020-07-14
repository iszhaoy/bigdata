package com.iszhaoy.review

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split(" "))
      // 过滤掉不为空的数据
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 启动流处理
    env.execute("stream wordcount")

  }
}
