package com.iszhaoy.table

import com.iszhaoy.apitest.SensorReading
import com.iszhaoy.table.Example.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{GroupWindow, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.UnboundedRange
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 14:52
 * @version 1.0
 */
object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("sernsor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    // 将流转换成表 直接定义时间字段
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
    //    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'et.rowtime)
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    //    sensorTable.printSchema()

    // 1 table api 实现
    // 1.1 GroupWindow聚合操作
    val resultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)
    // 打印输出 // 因为是在一个窗口结束时聚合好的数据，已经不会改变了，所以直接用追加模型和撤销模型都是可以的
    //    resultTable.toAppendStream[Row].print("agg")

    // 1.2 OverWindow
    val overResultTable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
    //    overResultTable.toAppendStream[Row].print()

    // 2. SQL实现
    // 2.1 GroupWindow
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      // tumble(ts,interval '4' second, interval '10' second)
      """
        | select id, count(id), hop_end(ts,interval '4' second, interval '10' second)
        | from sensor
        | group by id, hop(ts,interval '4' second, interval '10' second)
      """.stripMargin)

    //    resultSqlTable.toAppendStream[Row].print()


    // 2。2 OverWindow
    val overSqlTable: Table = tableEnv.sqlQuery(
      """
        | select
        |   id,
        |   ts,
        |   count(id) over w,
        |   avg(temperature) over w
        | from
        |   sensor
        | window w as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        | )
      """.stripMargin
    )
    overSqlTable.toAppendStream[Row].print()
    env.execute("job")
  }
}
