package com.iszhaoy.review

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._



// 批处理代码
object WordCount {
  def main(args: Array[String]): Unit = {

    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "flink-demo\\src\\main\\resources\\log.txt"
    val numLines: IntCounter = new IntCounter()
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)
    // 分词之后做count
    val wordCountDataset: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataset.print()
  }
}
