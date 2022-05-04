package com.iszhaoy.table.udf

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/3 10:47
 * @version 1.0
 */
object TableFunctionTest1 {
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

    val split = new Split("_")

    sensorTable
      .joinLateral(split('id) as ('word,'length)) // 侧向连接，应用TableFunction
      .select('id,'ts,'word,'length)
      .toAppendStream[Row].print("api")

    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("split",split)
    tableEnv.sqlQuery(
      """
        | select id,ts,temp.word,temp.length
        | from
        | sensor,lateral table(split(id)) as temp(word,length)
        |""".stripMargin
    ).toAppendStream[Row].print("sql")


    env.execute("job")

  }

  class Split(sep: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(sep).foreach(
        (word: String) => collect((word, word.length))
      )
    }
  }

}
