package com.iszhaoy.table.udf

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 18:21
 * @version 1.0
 */
object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("sensor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)
    // 使用自定义的hash函数
    val hashCode = new HashCode(0.50)
    sensorTable
      .select('id, 'ts, hashCode('id))
      .toAppendStream[Row].print("api")

    tableEnv.registerFunction("hashcode", hashCode)
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.sqlQuery(
      """
        |select id, ts, hashcode(id)
        |from sensor
        |""".stripMargin
    )
      .toAppendStream[Row].print("sql")

    env.execute("job")
  }

  // 自定义一个球hashcode的标量函数
  class HashCode(factor: Double) extends ScalarFunction {
    def eval(value: String): Int = {
      (value.hashCode * factor).toInt
    }
  }

}
