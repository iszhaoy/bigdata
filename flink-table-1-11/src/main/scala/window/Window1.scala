package window

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/8/7 14:08
 * @version 1.0
 */
object Window1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val fsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, fsSettings)

    // 定义kafka 订单表
    tEnv.executeSql(
      s"""
         |create table OrderTable (
         | `ts` bigint,
         | `orderId` string,
         | `userId` string,
         | `goodsId` int,
         | `price` double,
         | `cityId` int,
         | `procTime` AS proctime(),
         | `eventTime` AS to_timestamp(from_unixtime(`ts`,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for `eventTime` as `eventTime` - interval '5' second
         |) with (
         | 'connector' = 'kafka',
         | 'topic' = 'orderTopic',
         | 'properties.bootstrap.servers' = 'hadoop01:9092',
         | 'properties.group.id' = 'order_group',
         | 'format' = 'json',
         | 'scan.startup.mode' = 'latest-offset'
         | )
         |""".stripMargin)

    // 定义mysql 商品维度表
    tEnv.executeSql(
      s"""
         |create table GoodsTable (
         |  goodsId int,
         |  goodsName string
         |) with (
         |  'connector.type' = 'jdbc',
         |  'connector.url' = 'jdbc:mysql://localhost:3306/cumin',
         |  'connector.table' = 'goods',
         |  'connector.driver' = 'com.mysql.jdbc.Driver',
         |  'connector.username' = 'root',
         |  'connector.password' = 'root',
         |  'connector.lookup.cache.max-rows' = '2',
         |  'connector.lookup.cache.ttl' = '10s',
         |  'connector.lookup.max-retries' = '3'
         |)
         |""".stripMargin)

    //tEnv.sqlQuery(
    //  s"""
    //     |select * from GoodsTable
    //     |""".stripMargin)
    //    .toAppendStream[Row]
    //    .print("orderTable")

    // 关联商品维度，并求窗口10秒，最近5秒内商品维度分类coun, top 2
    val GoodsByNum: Table = tEnv.sqlQuery(
      s"""
         |select
         |    t.goodsId,
         |    t.goodsName,
         |    count(*) as byNum,
         |    HOP_START(t.eventTime,interval '5' second, interval '10' second) as wStart,
         |    HOP_End(t.eventTime,interval '5' second, interval '10' second) as wEnd
         |from (
         |    select
         |        o.orderId,
         |        o.userId,
         |        o.goodsId,
         |        g.goodsName,
         |        o.price,
         |        o.cityId,
         |        o.eventTime
         |    from
         |        OrderTable o left join  GoodsTable for system_time as of o.procTime AS g
         |    on o.goodsId = g.goodsId
         |) t
         |group by t.goodsId,t.goodsName,HOP(t.eventTime,interval '5' second, interval '10' second)
         |""".stripMargin)
    tEnv.createTemporaryView("GoodsByNum", GoodsByNum)

    // 统计top 2
    val ResultTable: Table = tEnv.sqlQuery(
      s"""
         |select
         |    t.goods_id,
         |    t.goods_name,
         |    t.by_num,
         |    t.w_start,
         |    t.w_end,
         |    t.rk
         |from (
         |    select
         |    goodsId as goods_id,
         |    goodsName as goods_name,
         |    byNum as by_num,
         |    wStart as w_start,
         |    wEnd as w_end,
         |    row_number() over(partition by wStart,wEnd order by byNum desc) as rk
         | from GoodsByNum
         |) t
         |where t.rk <=2
         |""".stripMargin)
    tEnv.createTemporaryView("ResultTable", ResultTable)

    tEnv.executeSql(
      s"""
         |create table GoodsTop2 (
         |  goods_id int PRIMARY KEY  NOT ENFORCED,
         |  goods_name string,
         |  by_num bigInt,
         |  w_start TIMESTAMP(3),
         |  w_end TIMESTAMP(3)
         |) with (
         |  'connector.type' = 'jdbc',
         |  'connector.url' = 'jdbc:mysql://localhost:3306/cumin',
         |  'connector.table' = 'goodstop2',
         |  'connector.driver' = 'com.mysql.jdbc.Driver',
         |  'connector.username' = 'root',
         |  'connector.password' = 'root',
         |  'connector.write.flush.max-rows' = '5000',
         |  'connector.write.flush.interval' = '2s',
         |  'connector.write.max-retries' = '3'
         |)
         |""".stripMargin
    )


    // 将结果落地到mysql
    //tEnv.executeSql(
    //  s"""
    //     |insert into GoodsTop2 select goods_id,goods_name,by_num,w_start,w_end from ResultTable
    //     |""".stripMargin)

    tEnv.sqlQuery(
      s"""
         |select * from ResultTable
         |""".stripMargin)
      .toRetractStream[Row]
      .print()

    env.execute()
  }
}
