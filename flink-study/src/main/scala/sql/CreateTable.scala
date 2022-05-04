package sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api._
/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/17 16:54
 * @version 1.0
 */
object CreateTable {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
      .useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    tableEnv.connect(new FileSystem().path(getClass.getResource("/data/sensor.txt").getPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamps", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable")
    val resultTable: Table = inputTable
      // 读一个view必须要有操作，才可以输出
      //.select($"id", $"timestamps", $"temperature")
      .filter($"id" === "sensor_1")
    resultTable
      .toAppendStream[(String, Long, Double)]
      .print("api")
    tableEnv.connect(new FileSystem().path("/out.txt"))
      .withFormat(new Csv().fieldDelimiter('|'))
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamps", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("outputTable")

    //resultTable.executeInsert("outputTable")

    tableEnv.createTemporaryView("sqlTable", inputTable)

    tableEnv.sqlQuery("select * from sqlTable").
      //toAppendStream[(String,Long,Double)]
      //toAppendStream[Row]
      // view 或者table中的字段必须与case class 一致
      toAppendStream[SensorReading]
      .print("sql")

    //tableEnv.executeSql(
    //  """
    //    |insert into outputTable
    //    |select * from sqlTable
    //    |where id = 'sensor_1'
    //    |""".stripMargin)

    // 必须使用env启动程序，不能使用TableEnv
    // 一旦 Table 被转化为 DataStream，必须使用 StreamExecutionEnvironment 的 execute 方法执行该 DataStream 作业。
    env.execute("createTable")

  }
}
