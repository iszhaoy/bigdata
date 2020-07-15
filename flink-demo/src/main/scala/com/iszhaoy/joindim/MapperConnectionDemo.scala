package com.iszhaoy.joindim

import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.alibaba.fastjson.JSON
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.MapListHandler
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.ExecutorUtils

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/13 16:11
 * @version 1.0
 */
object MapperConnectionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.socketTextStream("hadoop01", 9999)
      // 有状态算子一定要配置 uid
      .uid("order_topic_name")
      // 过滤为空的数据
      .filter(_ != null)
      // 解析数据
      .map(new MapperFunction())
      .print("mapperjoin")

    env.execute("MapperConnectionDemo")

  }
}

class MapperFunction() extends RichMapFunction[String, (Order, String)] {
  private var map: java.util.Map[Integer, String] = _
  var service: ScheduledExecutorService = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    map = new util.HashMap[Integer, String](1024)
    service = Executors.newScheduledThreadPool(1)
    service.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource

          val ds: MysqlConnectionPoolDataSource = new MysqlConnectionPoolDataSource
          ds.setURL("jdbc:mysql://localhost:3306/cumin")
          ds.setUser("root")
          ds.setPassword("root")
          val queryRunner = new QueryRunner(ds)
          val info: util.List[util.Map[String, AnyRef]] = queryRunner.query("select goodsId,goodsName from goods", new
              MapListHandler())
          import scala.collection.JavaConverters._
          for (i <- info.asScala) {
            val key: Int = i.get("goodsId").toString.trim.toInt
            val value: String = i.getOrDefault("goodsName", "").toString.trim
            map.put(key, value)
          }
          println(s"Thread Fetch ${info.size()} 条商品信息")
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }, 0, 1, TimeUnit.MINUTES)
  }

  override def map(value: String): (Order, String) = {
    val order: Order = JSON.parseObject(value, classOf[Order])
    (order, map.get(order.goodsId))
  }

  override def close(): Unit = {
    map.clear()
    ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, service)
    super.close
  }
}
