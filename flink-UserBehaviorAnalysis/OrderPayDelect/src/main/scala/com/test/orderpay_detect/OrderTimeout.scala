package com.test.orderpay_detect


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/13 12:01
 * @version 1.0
 */

// 定义输入样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出样例类
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")

    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("hadoop01", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)


    // 定义一个匹配模式 pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 把模式匹配用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 调用select方法提取时间序列,超时的时间要做报警提示
    val outderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream = patternStream
      .select(outderTimeoutOutputTag, new OrderTimeoutSelectFunction(), new OrderPaySelectFunction())


    resultStream.print("payed")
    resultStream.getSideOutput(outderTimeoutOutputTag).print("timeout")
    env.execute("OrderTimeout Job")

  }
}

// 自定义超时时间序列处理函数
class OrderTimeoutSelectFunction() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // l 为时间戳
    val beginEvent: OrderEvent = map.get("begin").iterator().next()
    OrderResult(beginEvent.orderId, "timeout")
  }
}

// 正常支付时间序列处理函数
class OrderPaySelectFunction() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("begin").iterator().next().orderId
    OrderResult(payedOrderId, "payed successful!")
  }
}
