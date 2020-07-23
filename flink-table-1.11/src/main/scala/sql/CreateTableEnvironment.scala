package sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/16 12:15
 * @version 1.0
 */
object CreateTableEnvironment {
  def main(args: Array[String]): Unit = {
    // **********************
    // FLINK STREAMING QUERY
    // **********************
    val fsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner()
      .inStreamingMode().build()
    val fsEnv: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val fsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(fsEnv, fsSettings)
    //val fsTableEnv: TableEnvironment = TableEnvironment.create(fsSettings)

    // ******************
    // FLINK BATCH QUERY
    // ******************
    val fbEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val fbTableEnv = BatchTableEnvironment.create(fbEnv)

    // **********************
    // BLINK STREAMING QUERY
    // **********************
    val bsEnv: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner()
      .inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    //val bsTableEnv = TableEnvironment.create(bsSettings)

    // ******************
    // BLINK BATCH QUERY
    // ******************
    val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val bbTableEnv = TableEnvironment.create(bbSettings)
  }
}
