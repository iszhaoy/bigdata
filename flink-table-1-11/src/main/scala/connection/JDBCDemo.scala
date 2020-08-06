package connection

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/24 16:24
 * @version 1.0
 */

case class City(cityId: Int, cityName: String)

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    //DataStream的JDBC连接器目前版本只包含了Sink
    //创建的JDBC接收器提供了至少一次的保证。使用upsert语句或幂等更新可以有效地精确地完成一次。
    env
      .fromElements[City](
        City(888, "吕梁"),
        City(889, "济南"),
        City(834, "湖南") // upsert
      )
    //.addSink(JdbcSink.sink(
    //  s"""
    //     |INSERT INTO citys
    //     |(cityId,cityName)
    //     |VALUES (?,?)
    //     |ON DUPLICATE KEY UPDATE
    //     |cityId = VALUES(cityId),
    //     |cityName = VALUES(cityName)
    //     |""".stripMargin,
    //  new JdbcStatementBuilder[City] {
    //    override def accept(ps: PreparedStatement, c: City) = {
    //      ps.setInt(1, c.cityId)
    //      ps.setString(2, c.cityName)
    //    }
    //  },
    //  new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
    //    .withUrl("jdbc:mysql://localhost:3306/cumin")
    //    .withDriverName("com.mysql.jdbc.Driver")
    //    .withUsername("root")
    //    .withPassword("root")
    //    .build()
    //))

    val tableEnvSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, tableEnvSettings)

    // 定义表结构
    tableEnv.executeSql(
      s"""
         | CREATE TABLE citys_mysql (
         |      cityId INT,
         |      cityName STRING,
         |      PRIMARY KEY (cityId) NOT ENFORCED
         |    ) WITH (
         |      'connector' = 'jdbc',
         |      'url' = 'jdbc:mysql://localhost:3306/cumin',
         |      'table-name' = 'citys',
         |      'username' = 'root',
         |      'password' = 'root',
         |      'lookup.cache.max-rows' = '100',
         |      'lookup.cache.ttl' = '10'
         |    )
         |""".stripMargin)
    // 查询
    val CityTableMysql: Table = tableEnv.sqlQuery(
      s"""
         |SELECT cityId,cityName
         |FROM
         |citys_mysql
         |""".stripMargin)

    CityTableMysql.toRetractStream[City].print("citys_mysql")

    env.execute("JDBCDemo")
  }
}
