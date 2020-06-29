package com.iszhaoy.table

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object Example {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = getClass.getClassLoader.getResource("sernsor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    // 基于env创建表环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于talbeEnv 将流转换为表
    val dataTable: Table = tableEnv.fromDataStream[SensorReading](dataStream)

    // 调用table api 做转换操作
    val resultTable: Table = dataTable
      .select("id,temperature")
      .filter("id == 'sensor_1'")
    // 写sql实现转换
    tableEnv.createTemporaryView("dataTable",dataTable)

    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from dataTable
        |where id = 'sensor_1'
        |""".stripMargin)

    // 把表转换成流，打印输出
    val resultStream: DataStream[(String, Double)] = resultSqlTable
      .toAppendStream[(String, Double)]

    resultStream.print()
    env.execute("Example")
  }
}
