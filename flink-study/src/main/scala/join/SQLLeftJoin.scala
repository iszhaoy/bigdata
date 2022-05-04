package join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.types.Row

object SQLLeftJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val leftStream = env.socketTextStream("localhost", 9998)
      .map(data => {
        val arrs = data.split(",")
        (arrs(0), arrs(1), arrs(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(2)) {
        override def extractTimestamp(t: (String, String, Long)): Long = t._3 * 1000L
      })

    leftStream.print("left")

    val rightStream = env.socketTextStream("localhost", 9999)
      .map(data => {
        val arrs = data.split(",")
        (arrs(0), arrs(1), arrs(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(1)) {
        override def extractTimestamp(t: (String, String, Long)): Long = t._3 * 1000L
      })

    tEnv.createTemporaryView("leftTable", leftStream, 'key, 'v, 'ts.rowtime)
    tEnv.createTemporaryView("rightTable", rightStream, 'key, 'v, 'ts.rowtime)

    val joinTable = tEnv.sqlQuery(
      s"""
         |SELECT l.key
         |       ,l.v
         |       ,r.v
         |FROM leftTable l
         |LEFT JOIN rightTable r
         |ON l.key = r.key and  r.ts between l.ts  and  l.ts + interval '10' second
         |""".stripMargin)
    joinTable.toRetractStream[Row]
      .print()

    env.execute("SQLLeftJoin")
  }
}
