package temporal

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/8/10 15:10
 * @version 1.0
 */

case class Order(ts: Long, amount: Int, currency: String)

case class RatesHistory(ts: Long, currency: String, rate: Int)

object TemporalTableFunc {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    // 提供一个汇率历史记录表静态数据集


    val orderStream: DataStream[Order] = env.readTextFile(getClass.getResource
    ("/data/Orders.txt").getPath)
      .map(line => {
        val arrs: Array[String] = line.split(",")
        Order(arrs(0).trim.toLong, arrs(1).trim.toInt, arrs(2).trim)
      }).assignAscendingTimestamps(_.ts * 1000L)


    val ratesHistoryStream: DataStream[RatesHistory] = env.readTextFile(getClass.getResource
    ("/data/history.txt").getPath)
      .map(line => {
        val arrs: Array[String] = line.split(",")
        RatesHistory(arrs(0).trim.toLong, arrs(1).trim, arrs(2).trim.toInt)
      }).assignAscendingTimestamps(_.ts * 1000L)


    val ratesHistoryTable: Table = tEnv.fromDataStream(ratesHistoryStream, 'rowtime.rowtime(), 'currency, 'rate)
    val orderTable: Table = tEnv.fromDataStream(orderStream, 'rowtime.rowtime(), 'amount, 'currency)

    tEnv.createTemporaryView("ratesHistoryTable", ratesHistoryTable)
    tEnv.createTemporaryView("orderTable", orderTable)

    // 创建和注册时态表函数
    // 为了访问时态表中的数据，必须传递一个时间属性，该属性确定将要返回的表的版本。 Flink 使用表函数的 SQL 语法提供一种表达它的方法。
    // 定义后，时态表函数将使用单个时间参数 timeAttribute 并返回一个行集合。 该集合包含相对于给定时间属性的所有现有主键的行的最新版本。
    // 在我们的示例中,每个记录从订单将与利率的版本时间o.rowtime。货币字段定义主键的利率之前和在我们的例子中是用来连接两个表。如果查询使用处理时间概念,新追加的订单总是与利率的最新版本执行操作。
    // 指定 "rowtime" 为时间属性，指定 "currency" 为主键
    val rates = ratesHistoryTable.createTemporalTableFunction($"rowtime", $"currency") // <==== (1)
    tEnv.registerFunction("Rates", rates) // <==== (2)

    tEnv.sqlQuery(
      s"""
         |SELECT
         |  o.currency,o.rowtime,o.amount,r.rate,o.amount * r.rate AS amount
         |FROM
         |  orderTable AS o,
         |  LATERAL TABLE (Rates(o.rowtime)) AS r
         |WHERE r.currency = o.currency
         |""".stripMargin)
      .toRetractStream[Row]
      .print()

    env.execute("TemporalTableFunc")
  }
}
