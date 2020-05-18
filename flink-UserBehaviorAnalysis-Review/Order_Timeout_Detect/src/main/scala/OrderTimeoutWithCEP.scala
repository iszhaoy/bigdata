import java.util

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
 * @date 2020/5/18 10:37
 * @version 1.0
 */

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)

object OrderTimeoutWithCEP {

  // 订单超时tag
  val orderTimeoutOutput: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeOut")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val orderEventStream = env.readTextFile(getClass.getResource("OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)
    // 定义一个带匹配时间窗口的模式
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签
    val patternStream = CEP.pattern(orderEventStream, pattern)

    val resultStream = patternStream.select(orderTimeoutOutput, new PatternTimeoutFunction[OrderEvent, OrderResult] {
      override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long) = {
        val event: OrderEvent = map.get("begin").iterator().next()
        OrderResult(event.orderId, "timeout")
      }
    }, new PatternSelectFunction[OrderEvent, OrderResult] {
      override def select(map: util.Map[String, util.List[OrderEvent]]) = {
        val event: OrderEvent = map.get("follow").iterator().next()
        OrderResult(event.orderId, "payed succcessful")
      }
    })


    resultStream.getSideOutput(orderTimeoutOutput)
      .print("timeout")

    resultStream.print("pay")

    env.execute("OrderTimeout")
  }
}

