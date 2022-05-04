package hive

import java.time.Duration

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/17 16:53
 * @version 1.0
 */
object FlinkOnHive {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val tableEnvSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))

    // 注册HiveCatalog
    val catalogName = "my_catalog"
    val catalog = new HiveCatalog(
      catalogName,              // catalog name
      "default",                // default database
      "D:\\workspace\\iszhaoy\\bigdata\\bigdata\\flink-table-1.11\\src\\main\\resources",  // Hive config (hive-site.xml) directory
      "1.2.0"                   // Hive version
    )
    tableEnv.registerCatalog(catalogName, catalog)
    tableEnv.useCatalog(catalogName)

    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS stream_tmp")
    tableEnv.executeSql("DROP TABLE IF EXISTS stream_tmp.order_log_kafka")

    tableEnv.executeSql(
      s"""
         |CREATE TABLE stream_tmp.order_log_kafka (
         | `time` BIGINT,
         | orderId STRING,
         | userId STRING,
         | goodsId BIGINT,
         | price BIGINT,
         | cityId BIGINT,
         | procTime AS PROCTIME(),
         | eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(`time`,'yyyy-MM-dd HH:mm:ss')),
         | watermark for eventtime as eventtime - interval '1' second
         |) with (
         | 'connector' = 'kafka',
         | 'topic' = 'orderTopic',
         | 'properties.bootstrap.servers' = 'hadoop01:9092',
         | 'properties.group.id' = 'iszhaoy-2020-07-23',
         | 'format' = 'json',
         | 'scan.startup.mode' = 'earliest-offset',
         | 'json.fail-on-missing-field' = 'false',
         | 'json.ignore-parse-errors' = 'true'
         |)
         |
         |""".stripMargin
    )

    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    tableEnv.executeSql("DROP TABLE IF EXISTS stream_tmp.order_log_hive")

    tableEnv.executeSql(
      """
        |CREATE TABLE stream_tmp.order_log_hive (
        |  TUMBLE_END STRING,
        |  order_count BIGINT
        |) PARTITIONED BY (
        |  ts_date STRING,
        |  ts_hour STRING,
        |  ts_minute STRING,
        |  ts_scond STRING
        |) STORED AS PARQUET
        |TBLPROPERTIES (
        |  'sink.partition-commit.trigger' = 'partition-time',
        |  'sink.partition-commit.delay' = '10s',
        |  'sink.partition-commit.policy.kind' = 'metastore,success-file',
        |  'partition.time-extractor.timestamp-pattern' = '$ts_date $ts_hour:$ts_minute:$ts_scond'
        |)
  """.stripMargin
    )

    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    tableEnv.executeSql(
      s"""
         |INSERT INTO stream_tmp.order_log_hive
         |SELECT
         |DATE_FORMAT(TUMBLE_END(eventTime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss'),
         |count(*),
         |DATE_FORMAT(TUMBLE_END(eventTime, INTERVAL '10' SECOND),'yyyy-MM-dd'),
         |DATE_FORMAT(TUMBLE_END(eventTime, INTERVAL '10' SECOND),'HH'),
         |DATE_FORMAT(TUMBLE_END(eventTime, INTERVAL '10' SECOND),'mm'),
         |DATE_FORMAT(TUMBLE_END(eventTime, INTERVAL '10' SECOND),'ss')
         |FROM
         |  stream_tmp.order_log_kafka
         |GROUP BY TUMBLE(eventTime, INTERVAL '10' SECOND)
         |""".stripMargin)

    streamEnv.execute("hive job")

  }
}
