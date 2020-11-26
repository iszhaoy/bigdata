package join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object SQLLeftJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val leftStream = env.socketTextStream("localhost", 9998)
      .map(data => {
        val arrs = data.split(",")
        (arrs(0), arrs(1), arrs(2).toLong)
      })
      .assignAscendingTimestamps(_._3 * 1000L)

    val rightStream = env.socketTextStream("localhost", 9999)
      .map(data => {
        val arrs = data.split(",")
        (arrs(0), arrs(1), arrs(2).toLong)
      })
      .assignAscendingTimestamps(_._3 * 1000L)

    tEnv.createTemporaryView("leftTable", leftStream, 'key, 'v, 'ts.rowtime)
    tEnv.createTemporaryView("rightTable", rightStream, 'key, 'v, 'ts.rowtime)

    val joinTable = tEnv.sqlQuery(
      s"""
         |SELECT l.key
         |       ,l.v
         |       ,r.v
         |FROM leftTable l
         |LEFT JOIN rightTable r
         |ON l.key = r.key
         |""".stripMargin)
//    -- and r.ts between l.ts  and  l.ts + interval '10' second
    joinTable.toRetractStream[Row]
      .print()

    env.execute("SQLLeftJoin")
  }
}
