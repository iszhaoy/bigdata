package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

import scala.collection.mutable

object ROW_NUMBER_TEST {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
      .useBlinkPlanner().build()
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)
    // {"CREATE_TIME":1547718195,"ORDER_ID":"1001","O_NO":"2"}
    // {"CREATE_TIME":1547718205,"ORDER_ID":"1001","O_NO":"3"}
    // {"CREATE_TIME":1547718214,"ORDER_ID":"1001","O_NO":"4"}
    // {"CREATE_TIME":1547718198,"ORDER_ID":"1001","O_NO":"1"}
    // {"CREATE_TIME":1547718198,"ORDER_ID":"1002","O_NO":"1"}
    tEnv.executeSql(
      s"""
         |CREATE TABLE TEST_ZY (
         | CREATE_TIME BIGINT,
         | ORDER_ID STRING,
         | O_NO STRING,
         | proctime AS PROCTIME(),
         | EVENTTIME AS TO_TIMESTAMP(FROM_UNIXTIME(`CREATE_TIME`,'yyyy-MM-dd HH:mm:ss')),
         | WATERMARK FOR EVENTTIME AS EVENTTIME - INTERVAL '1' SECOND
         |) WITH (
         | 'connector' = 'kafka',
         | 'topic' = 'orderTopic',
         | 'properties.bootstrap.servers' = '192.168.149.80:9092',
         | 'properties.group.id' = 'iszhaoy-2020-10-30',
         | 'format' = 'json',
         | 'scan.startup.mode' = 'latest-offset',
         | 'json.fail-on-missing-field' = 'false',
         | 'json.ignore-parse-errors' = 'true'
         |)
         |""".stripMargin
    )

    val CONCAT_UDF = new CONCAT_UDF()
    tEnv.registerFunction("CONCAT_UDF", CONCAT_UDF)
    tEnv.sqlQuery(
      s"""
         | SELECT ORDER_ID,CONCAT_UDF(O_NO) as udf, TUMBLE_END(proctime, INTERVAL '10' SECOND) as proctime
         | FROM
         |  TEST_ZY
         | GROUP BY ORDER_ID,TUMBLE(proctime, INTERVAL '10' SECOND)
         |""".stripMargin
    )
      .toAppendStream[Row]
      .print()
    env.execute("order_test")

  }

  class CONCAT_UDF extends AggregateFunction[String, StringBuilder] {
    def accumulate(acc: StringBuilder, temp: String): mutable.Seq[Char] = {
      acc.append("|").append(temp)
    }
    override def getValue(accumulator: StringBuilder): String = accumulator.toString()
      .substring(1)

    override def createAccumulator(): StringBuilder = new StringBuilder()
  }

}
