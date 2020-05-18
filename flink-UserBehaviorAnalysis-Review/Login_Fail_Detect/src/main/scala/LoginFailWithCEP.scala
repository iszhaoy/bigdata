import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author zhaoyu
 * @date 2020/5/17
 */

// 输出的异常报警信息
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile(getClass.getResource("LoginLog.csv").getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(element: LoginEvent) = element.eventTime * 1000L
      })
      .keyBy(_.userId)


    // 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))


    // 在数据流中匹配定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream, loginFailPattern)

    // .select方法传入一个pattern select function ， 当检测到定义好的模式序列是就会调用
    patternStream
      .select(new LogingFailMatch())
      .print("warn")
    env.execute("login fail alert")
  }
}

class LogingFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {

    // map中就是每个匹配模式的k v，k是标签，v是stream
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()

    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}