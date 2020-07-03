package com.iszhaoy.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/1 17:39
 * @version 1.0
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.1 老版本planner的流处理
    //    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useOldPlanner() // 使用老版本
    //      .inStreamingMode() // 使用流处理模式
    //      .build()
    //    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    //
    //    // 1.2 老版本批处理环境
    //    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
    //    // ========================================================================================
    //    // 1.3 blink版本的流处理
    //    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inStreamingMode()
    //      .build()
    //    val bsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)
    //
    //    // 1.4 blink版本的批处理
    //    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inBatchMode()
    //      .build()

    //    val bbTableEnv = TableEnvironment.create(bbSettings)

    // 2. 连接外部系统读取数据
    // 2.1 读取文件数据
    val filePath: String = getClass.getResource("/sernsor.txt").getPath

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) // 定义了从外部文件读取数据之后的格式化 （反序列化）
      .withSchema(new Schema() // 定义表结构
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable") // 在表环境注册一张表

    // 测试输出
//    val inputTable: Table = tableEnv.from("inputTable")
//    inputTable.toAppendStream[(String, Long, Double)]
//      .print("inputTable")


    // 2.. 消费kafka数据
//    tableEnv.connect(new Kafka()
//      .version("0.11") // 定义版本
//      .topic("sensor") // 定义主题
//      .property("zookeeper.connect", "hadoop01:2181")
//      .property("bootstrap.servers", "hadoop01:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema() // 定义表结构
//        .field("id", DataTypes.STRING())
//        .field("timestamp", DataTypes.BIGINT())
//        .field("temperature", DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaInputTable")
//
//    tableEnv.from("kafkaInputTable")
//      .toAppendStream[(String, Long, Double)]
//      .print("kafkaInputTable")

    // 3. 表的查询转换
    val sensorTable: Table = tableEnv.from("inputTable")
    // 3.1 简单查询转换
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    // 3.2 聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

//    tableEnv.createTemporaryView("resultTable",resultTable)

    val  aggResultSqlTable:Table = tableEnv.sqlQuery(
      s"""
        |select id,count(*) from
        |${resultTable}
        |group by
        | id
        |""".stripMargin)
    aggResultSqlTable
        .toRetractStream[(String,Long)]
        .print("aggResultSqlTable")


    resultTable
      .toAppendStream[(String, Double)]
      .print("resultTable")

    aggResultTable
      .toRetractStream[(String, Long)]
      .print("aggResultTable")

    env.execute("test")
  }
}
