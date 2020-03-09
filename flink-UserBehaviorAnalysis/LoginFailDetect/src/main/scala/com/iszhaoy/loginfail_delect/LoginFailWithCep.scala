package com.iszhaoy.loginfail_delect

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/12 16:56
 * @version 1.0
 */

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("/LoginLog.csv")

    // 1. 读取事件数据，创建简单事件流
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent) = {
          element.eventTime * 1000L
        }
      })
      .keyBy(_.userId)

    // 2. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 3. 在事件流上应用模式,得到PatternStrem
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    // 3. 从pattern stream 上应用select function 检出匹配事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute("LoginFailWithCep")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // map中就是每个匹配模式的k v  k是标签 v就是stream
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}


