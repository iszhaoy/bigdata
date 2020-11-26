package com.iszhaoy.table

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.types.Row


object TestCsvReader2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val dataStream = env.readTextFile(getClass.getResource("/goods.log").getPath)

    val t1 = tableEnv.fromDataStream(dataStream,'A)

    tableEnv.sqlQuery(
      s"""
         |
         |select '"'||REPLACE(A,'"','""')||'"' from $t1
         |
         |""".stripMargin)
        .toAppendStream[Row]
        .print("test")

    env.execute("testCsvReader")
  }
}
