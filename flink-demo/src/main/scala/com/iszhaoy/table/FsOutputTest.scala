package com.iszhaoy.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 10:58
 * @version 1.0
 */
object FsOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.connect(new FileSystem().path(getClass.getResource("/sernsor.txt").getPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamps", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable")

    // 查询
    val filterTable: Table = inputTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    val aggResultTable: Table = inputTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    //5. 将结果表输出到文件中
    tableEnv.connect(new FileSystem().path("D:\\workspace\\iszhaoy\\bigdata\\bigdata\\flink-demo\\src\\main\\resources" +
      "\\output.txt"))
        .withFormat(new Csv)
        .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("temperature", DataTypes.DOUBLE())
        ).createTemporaryTable("outputTable")
    filterTable.insertInto("outputTable")

    tableEnv.connect(new FileSystem().path("D:\\workspace\\iszhaoy\\bigdata\\bigdata\\flink-demo\\src\\main\\resources" +
      "\\aggresult.txt"))
      .withFormat(new Csv)
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("count", DataTypes.BIGINT())
      ).createTemporaryTable("aggResultTable")
    aggResultTable.insertInto("aggResultTable")


    env.execute("job")
  }
}
