package com.iszhaoy.table

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.types.Row

//A,test
//A,[]
//
//
//B,B
//B,[{"k1":"v1"},{"k2":"v2"}]
//
//
//C,B
//C,"[{""k1"":""v1""},{""k2"":""v2""}]"

object TestCsvReader {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    env.setParallelism(1)

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test_csv")
      .property("zookeeper.connect", "test:2181")
      .property("bootstrap.servers", "bigdata01:9092")
    )
      .withFormat(new Csv().ignoreParseErrors())
      .withSchema(new Schema()
        .field("A", DataTypes.STRING())
        .field("B", DataTypes.STRING())
      ).createTemporaryTable("T1")

//    tableEnv.sqlQuery("select * from T1")
//      .toAppendStream[Row].print()

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test_csv_b")
      .property("zookeeper.connect", "bigdata01:2181")
      .property("bootstrap.servers", "bigdata01:9092")
    )
      .withFormat(new Csv().ignoreParseErrors())
      .withSchema(new Schema()
        .field("C", DataTypes.STRING())
        .field("D", DataTypes.STRING())
      ).createTemporaryTable("T2")
//
//
    val joinTable = tableEnv.sqlQuery(
      s"""
         |select * from T1 a join T2 b
         |on a.A = b.C
         |""".stripMargin)
    .toAppendStream[Row]
    .print()

    env.execute("testCsvReader")
  }
}
