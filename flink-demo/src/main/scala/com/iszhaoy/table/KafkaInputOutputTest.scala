package com.iszhaoy.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 14:18
 * @version 1.0
 */
object KafkaInputOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("flink-table-source")
      .property("zookeeper.connect", "hadoop01:2181")
      .property("bootstrap.servers", "hadoop01:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamps", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("flink-table-sink")
      .property("zookeeper.connect", "hadoop01:2181")
      .property("bootstrap.servers", "hadoop01:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("outputTable")

    tableEnv.from("inputTable")
      .select('id, 'temperature)
      .filter('id === "sensor_1")
      .insertInto("outputTable")

    env.execute("job")
  }
}
