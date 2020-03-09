package com.iszhaoy.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/11 12:21
 * @version 1.0
 */

// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object Network {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("flink-UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log")
      .map((data: String) => {
        val dataArray: Array[String] = data.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val timestamp: Long = dateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      //      .filter(!_.url.matches("""^[0-9a-zA-Z_-]*.(png|css)$"""))
      .filter(!_.url.endsWith(".png"))
      .filter(!_.url.endsWith(".css"))
      .filter(!_.url.endsWith(".ico"))
      .filter(!_.url.endsWith(".js"))
      .filter(!_.url.endsWith(".jpeg"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent) = {
          element.eventTime
        }
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())

      .keyBy(_.windowEnd)
      .process(new TopNHostUrls(5))

    dataStream.print()

    env.execute("network_analysis")
  }

}

// 对url分组聚合
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口处理函数，聚合函数之后返回的输出流类型
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义排序输出处理函数
class TopNHostUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[UrlViewCount]("url_state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 从状态中拿到所有数据
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]

    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (iter.hasNext) {
      allUrlViews += iter.next()
    }

    urlState.clear()

    //    val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val sortedUrlViews = allUrlViews.sortWith((c1, c2) => {
      c1.count > c2.count
    }).take(topSize)

    // 格式化结果输出
    val result: StringBuilder = new StringBuilder()

    result.append("时间，").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrlViews.indices) {
      val currentUrlView = sortedUrlViews(i)
      result.append("No").append(i + 1).append(":")
        .append(" URL:").append(currentUrlView.url)
        .append(" 访问量:").append(currentUrlView.count).append("\n")
    }
    result.append("==================\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
