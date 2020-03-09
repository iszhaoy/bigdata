package com.iszhaoy.market_analysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/12 14:30
 * @version 1.0
 */

// 输入广告点击事件的样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 输入结果，根据省份划分出来的count值
case class CountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  // 定义侧输出流的标签
  val blackListOutPutTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("/AdClickLog.csv")

    val adEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickLog(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4)
          .trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickLog](Time.seconds(1)) {
        override def extractTimestamp(element: AdClickLog) = {
          element.timestamp * 1000
        }
      })
    // 黑名单过滤 - 自定义process function 过滤大量的点击的行为

    val filterBlackListUserStream = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))

    // 根据省份做分组,开窗聚合,如果需要多个key，包成元组
    val adCountStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCounting(), new AdCountResult())

    adCountStream.print("count")
    filterBlackListUserStream.getSideOutput(blackListOutPutTag).print("blackList")

    env.execute("AdStatisticsByGeo")

  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext
      .getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))

    // 保存定时器出发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog,
      AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
      // 取出count状态
      val curCount: Long = countState.value()

      // 如果是第一次处理，注册定时器,每天0点触发
      if (curCount == 0) {
        // 从1970年到现在的天数 + 1
        val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * 1000 * 60 * 60 * 24
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if (curCount >= maxCount) {
        // 判断是否发送过黑名单
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutPutTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times " +
            "today!!!"))
        }
        // 然后主流数据中就不要它了
        return
      }
      // 当前计数状态 + 1
      countState.update(curCount + 1L)
      out.collect(value)
    }

    // 定时器触发时，清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                         out: Collector[AdClickLog]): Unit = {
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

}

class AdCounting() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}


