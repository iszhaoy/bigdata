package sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table,_}
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/16 14:04
 * @version 1.0
 */
object CreateTable2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
      .useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    val dataStream1: DataStream[(String, Long, Double)] = env.readTextFile(getClass.getResource("/data/sensor.txt").getPath)
      .map(data => {
        val dataArrs: Array[String] = data.split(",")
        (dataArrs(0).trim, dataArrs(1).trim.toLong, dataArrs(2).trim.toDouble)
      })

    val dataStream2: DataStream[SensorReading] = env.readTextFile(getClass.getResource("/data/sensor.txt").getPath)
      .map(data => {
        val dataArrs: Array[String] = data.split(",")
        SensorReading(dataArrs(0).trim, dataArrs(1).trim.toLong, dataArrs(2).trim.toDouble)
      })

    //val table1: Table = tableEnv.fromDataStream(dataStream1)
    //val table1: Table = tableEnv.fromDataStream(dataStream1, $"_1" as "m1", $"_2" as "m2")
    // 可以交换字段
    val table1: Table = tableEnv.fromDataStream(dataStream1, $"_2" as "m2", $"_1" as "m1")
    val table2: Table = tableEnv.fromDataStream(dataStream1, $"a", $"b", $"c")
    val table3: Table = tableEnv.fromDataStream(dataStream1, $"a", $"b")

    table1.printSchema()
    table2.printSchema()
    table3.printSchema()
    table1.toAppendStream[Row].print()

    val table4: Table = tableEnv.fromDataStream(dataStream2)
    val table5: Table = tableEnv.fromDataStream(dataStream2, $"a", $"b", $"c")
    //val table6: Table = tableEnv.fromDataStream(dataStream2, $"a", $"b")
    val table6: Table = tableEnv.fromDataStream(dataStream2, $"timestamp", $"id")

    table4.printSchema()
    table5.printSchema()
    table6.printSchema()

    env.execute("createTable")

  }
}
