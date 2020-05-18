import java.lang

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.event.LoggingEvent

import scala.collection.mutable.ListBuffer

/**
 * @author zhaoyu
 * @date 2020/5/17
 */

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
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

    dataStream.print("source data")

    dataStream
      .keyBy(_.userId)
      .process(new MyKeyedProcessFunction(2))
      .print("alert")

    env.execute("login fail alert")
  }
}

class MyKeyedProcessFunction(sconds: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

  lazy val loginListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]
  ("loginListState", classOf[LoginEvent]))
  lazy val tsValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("tsValueState",
    classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context,
                              out: Collector[LoginEvent]): Unit = {

    if (value.eventType == "fail") {
      // 来一次登录失败的请求，就保存在list中
      // 将事件加入
      loginListState.add(value)

    }
    // 定时器 2秒后
    val ts = value.eventTime * 1000L + sconds * 1000L
    tsValueState.update(ts)
    ctx.timerService().registerEventTimeTimer(ts)
    //
    //    else if (value.eventType == "success") {
    //      ctx.timerService().deleteProcessingTimeTimer(tsValueState.value())
    //      loginListState.clear()
    //      tsValueState.clear()
    //    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext,
                       out: Collector[LoginEvent]): Unit = {
    val events: lang.Iterable[LoginEvent] = loginListState.get()
    import scala.collection.JavaConversions._
    val list: ListBuffer[LoginEvent] = ListBuffer()
    for (e <- events) {
      list.add(e)
    }
    loginListState.clear()
    tsValueState.clear()
    // 当list中的数量大于2，就报警
    if (list.size > 1) {
      out.collect(list.head)
    }
  }
}