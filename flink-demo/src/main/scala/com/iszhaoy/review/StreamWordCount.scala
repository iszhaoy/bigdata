package com.iszhaoy.review

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    //val host: String = params.get("host")
    val host: String = "hadoop01"
    //val port: Int = params.getInt("port")
    val port: Int = 9999

    // 创建一个流处理执行环境
    //val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())

    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split(" "))
      // 过滤掉不为空的数据
      .filter(_.nonEmpty)
      .map(new MyMapper)
      .keyBy(0)
      .sum(1)

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 启动流处理
    env.execute("stream wordcount")
  }
}

class MyMapper extends RichMapFunction[String, (String, Int)] {
  private var numsLines: IntCounter = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    numsLines = new IntCounter()
    getRuntimeContext.addAccumulator("nums-lines", this.numsLines)
  }

  override def map(value: String): (String, Int) = {
    this.numsLines.add(1)
    (value, 1)
  }
}

