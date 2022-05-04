import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

import java.time.Duration

object TEST_FileSystem {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode().useBlinkPlanner().build()
    System.setProperty("HADOOP_USER_NAME", "iszhaoy")
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(1))

    val catalogName = "myhive"
    val catalog = new HiveCatalog(
      catalogName, // catalog name
      "flink_meta", // default database
      "/opt/workspace/javaproject/bigdata/flink-study/src/main/resources/", // Hive config (hive-site.xml) directory
      "2.3.6" // Hive version
    )
    tEnv.registerCatalog(catalogName, catalog)
    tEnv.useCatalog(catalogName)
    tEnv.executeSql(s"""drop table orders_hdfs""")
    val orders_hdfs =
      s"""
         |create table if not exists orders_hdfs
         |(
         |    id       string,
         |    currency string,
         |    dt           string,
         |    `hour`       string,
         |    `min`  string
         |) partitioned by (dt, `hour`,`min`) with (
         |  'connector'='filesystem',
         |  'path'='hdfs://bigdata01:9000/sql_client/orders_fs',
         |  'format'='orc',
         |  'sink.partition-commit.delay' = '1min',
         |  'sink.partition-commit.trigger'='partition-time',
         |  'sink.partition-commit.policy.kind' = 'success-file'
         |)
         |""".stripMargin

    tEnv.executeSql(orders_hdfs)
    //
    val dml =
      s"""
         |SELECT id, currency, DATE_FORMAT(rowtime, 'yyyy-MM-dd'), DATE_FORMAT(rowtime, 'HH'), DATE_FORMAT(rowtime, 'mm')
         |FROM orders_kafka
         |""".stripMargin
    val resultTable = tEnv.sqlQuery(dml)
    resultTable.toAppendStream[Row].print("sink")

    resultTable.executeInsert("orders_hdfs")

    env.execute("TEST_FileSystem")

  }
}
