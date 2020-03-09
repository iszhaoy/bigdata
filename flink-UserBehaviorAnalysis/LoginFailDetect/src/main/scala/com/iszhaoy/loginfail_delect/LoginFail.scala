package com.iszhaoy.loginfail_delect

import java.lang
import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/12 16:56
 * @version 1.0
 */

//输入的登陆时间样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val resource: URL = getClass.getResource("/LoginLog.csv")

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


    //
    val warningStream = loginEventStream
      .keyBy(_.userId) // 以用户id做分组
      .process(new LoingWarining(2))


    warningStream.print()

    env.execute("LoginFail")
  }
}


class LoingWarining(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 保存2秒内的所有登陆失败的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext
    .getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

  // 来一个时间处理一次
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {
    if (value.eventType == "fail") {
      // 如果是失败，判断之前是否有登陆失败事件

      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        val firstFal = iter.next()

        if (value.eventTime < firstFal.eventTime + 2) {
          // 如果两次间隔小于两秒，输出报警
          out.collect(Warning(value.userId, firstFal.eventTime, value.eventTime, "login fail in 2 sconds "))
        }
        // 更新最近一次的登陆失败事件
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果是第一次登陆事变，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      loginFailState.clear()
    }
  }

}


class LoingWarining2(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 保存2秒内的所有登陆失败的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext
    .getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

  // 来一个时间处理一次
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {
    // 判断类型是否为fail，只添加fail的时间到状态
    val loginFailList: lang.Iterable[LoginEvent] = loginFailState.get()
    if (value.eventType == "fail") {
      if (!loginFailList.iterator().hasNext) {
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
      }
      loginFailState.add(value)
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
                       out: Collector[Warning]): Unit = {
    // 出发定时器的时候，根据状态里的失败个数决定是否触发
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter = loginFailState.get().iterator()

    while (iter.hasNext) {
      allLoginFails += iter.next()
    }

    if (allLoginFails.length >= maxFailTimes) {
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime,
        "login fail in 2 sconds for " + allLoginFails.length))

    }
    // 清空窗台
    loginFailState.clear()
  }
}
