package com.iszhaoy.joindim

import java.util.Collections
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.google.common.cache.{Cache, CacheBuilder}
import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLClient, SQLConnection}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/8 15:26
 */

case class OrderJoinDimRecord(time: Long, orderId: String, userId: String, goodsId: Int, var goodsName: String, price: Int, cityId: Int,
                              var cityName: String)

object ConnectionWithGuavaCacheVertxDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val orderStream: DataStream[OrderJoinDimRecord] =
      env.socketTextStream("hadoop01", 9999)
        // 有状态算子一定要配置 uid
        .uid("order_topic_name")
        // 过滤为空的数据
        .filter(_ != null)
        // 解析数据
        .map(str => JSON.parseObject(str, classOf[OrderJoinDimRecord]))

    val recordStream: DataStream[OrderJoinDimRecord] = AsyncDataStream.unorderedWait[OrderJoinDimRecord, OrderJoinDimRecord](orderStream,
      new MySQLDimensionAsyncFunc(), 1000, TimeUnit.MILLISECONDS, 100)

    recordStream.print("recordStream")

    env.execute("GuavaCacheVertxDemo")
  }
}

class MySQLDimensionAsyncFunc extends RichAsyncFunction[OrderJoinDimRecord, OrderJoinDimRecord] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[MySQLDimensionAsyncFunc])
  @transient private var sqlClient: SQLClient = _
  @transient
  @volatile private var dimCache: Cache[String, String] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val vertx: Vertx = Vertx.factory.vertx(
      new VertxOptions()
        .setWorkerPoolSize(10)
        .setEventLoopPoolSize(5)
    )
    val config: JsonObject = new JsonObject()
      .put("url", "jdbc:mysql://localhost:3306/cumin")
      .put("driver_class", "com.mysql.jdbc.Driver")
      .put("max_pool_size", 5)
      .put("user", "root")
      .put("password", "root")
    sqlClient = JDBCClient.createShared(vertx, config)

    dimCache = CacheBuilder.newBuilder()
      // initialCapacity()方法和maximumSize()方法分别指定该缓存的初始容量和最大容量，推荐对它们有一个预估
      .initialCapacity(10000)
      .maximumSize(20000)
      // Guava Cache的过期/刷新策略有3种
      // expireAfterWrite()：指定数据被写入缓存之后多久过期；
      // expireAfterAccess()：指定数据多久没有被访问过之后过期；
      // refreshAfterWrite()：指定数据被写入缓存之后多久刷新其值（不删除）。
      .expireAfterAccess(1, TimeUnit.MINUTES)
      // 指定k v的类型
      .build[String, String]()
  }

  override def asyncInvoke(order: OrderJoinDimRecord, resultFuture: ResultFuture[OrderJoinDimRecord]): Unit = {
    var needEnriching: Boolean = false
    val goodsId: Int = order.goodsId
    val cityId: Int = order.cityId

    // 此key有妙用，米奇妙妙屋，妙极了
    val goodsCacheKey: String = "g" + goodsId
    val cityCacheKey: String = "c" + cityId

    val selectSql: ListBuffer[String] = new ListBuffer[String]()

    // 缓存存在/缓存不存在准备sql阶段
    if (goodsId > 0) {
      val goodsName: String = dimCache.getIfPresent(goodsCacheKey)
      if (goodsName == null) {
        selectSql +=
          s"""
             |SELECT 'g' AS t,goodsName AS n FROM cumin.goods WHERE goodsId = $goodsId
             |""".stripMargin
        needEnriching = true
      } else {
        order.goodsName = goodsName
      }
    }
    if (cityId > 0) {
      val cityName: String = dimCache.getIfPresent(cityCacheKey)
      if (cityName == null) {
        selectSql +=
          s"""
             |SELECT 'c' AS t,cityName AS n FROM cumin.citys WHERE cityId = $cityId
             |""".stripMargin
      } else {
        order.cityName = cityName
      }
    }
    import scala.collection.JavaConverters._

    // 查库并维度关联阶段
    if (needEnriching) {
      sqlClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
        override def handle(connResult: AsyncResult[SQLConnection]): Unit = {
          if (connResult.failed()) {
            logger.error("Cannot get MySQL connection via Vertx JDBC client", connResult.cause())

            return
          }
          val conn: SQLConnection = connResult.result()
          val sql: String = StringUtils.join(selectSql.asJava, " UNION ALL ")
          logger.debug("sql is : " + sql)

          conn.query(sql, new Handler[AsyncResult[ResultSet]] {
            override def handle(queryResult: AsyncResult[ResultSet]): Unit = {
              if (queryResult.failed()) {
                logger.error(s"""Error executing SQL query: $sql ${queryResult.cause()}""")
                conn.close()
                return
              }

              val resultSet: ResultSet = queryResult.result()
              for (row <- resultSet.getRows.asScala) {
                val tag: String = row.getString("t")
                val name: String = row.getString("n")

                tag match {
                  case "g" => {
                    order.goodsName = name
                    dimCache.put(goodsCacheKey, name)
                  }
                  case "c" => {
                    order.cityName = name
                    dimCache.put(cityCacheKey, name)
                  }
                  case _ => logger.error("error cache key")
                }
              }

              resultFuture.complete(Collections.singletonList(order).asScala)
              conn.close()
            }
          })
        }
      })
    } else {
      resultFuture.complete(Collections.singletonList(order).asScala)
    }
  }

  override def close(): Unit = {
    super.close()
    sqlClient.close()
    dimCache.invalidateAll()
  }
}