import java.{lang, util}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author zhaoyu
 * @date 2020/5/8
 */

// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlWindowCount(url: String, windowEnd: Long, count: Long)

object NetWorkAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("apache.log")
    env.readTextFile(resource.getPath)
      .map(data => {
        // 83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
        val dataArray: Array[String] = data.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss")
        val timestamp: Long = sdf.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      // 将杂质过滤
      .filter(_.url.matches("^.*\\.(?:(?!(jpg|css|js|jpeg|ico|png|gif|jar|ttf)).)+$"))
      // 根据url分组并窗口聚合
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5)))
      .aggregate(
        // 指定聚合规则
        new AggregateFunction[ApacheLogEvent, Long, Long] {
          override def createAccumulator(): Long = 0L

          override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1L

          override def getResult(acc: Long): Long = acc

          override def merge(acc: Long, acc1: Long): Long = acc + acc1
        },
        // 指定窗口输出数据结构
        new WindowFunction[Long, UrlWindowCount, String, TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlWindowCount]): Unit = {
            out.collect(UrlWindowCount(key, window.getEnd, input.iterator.next()))
          }
        })
      // 根据窗口分组
      .keyBy(_.windowEnd)
      // 将同组的数据，存入liststate，并注册定时器，定时器出发的时候取出，排序 拿到top n,并清空定时器
      .process(new KeyedProcessFunction[Long, UrlWindowCount, String] {

        lazy val valueListState: ListState[UrlWindowCount] = getRuntimeContext
          .getListState(new ListStateDescriptor[UrlWindowCount]("valueList", classOf[UrlWindowCount]))

        lazy val timeServState: ValueState[Int] = getRuntimeContext
          .getState(new ValueStateDescriptor[Int]("timeServ", classOf[Int]))

        override def processElement(value: UrlWindowCount, ctx: KeyedProcessFunction[Long, UrlWindowCount, String]#Context, out: Collector[String]): Unit = {
          // 每进来一个元素，都存入listState中
          valueListState.add(value)

          if (timeServState.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1000L)
            timeServState.update(1)
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlWindowCount, String]#OnTimerContext, out: Collector[String]): Unit = {
          val sb = new StringBuilder
          // 遍历listState到ListBuffer,排序，求Top n
          val urls: lang.Iterable[UrlWindowCount] = valueListState.get()

          val urlList: ListBuffer[UrlWindowCount] = new ListBuffer[UrlWindowCount]
          // import scala.collection.JavaConversions._
          // for (url <- urls) {
          //   urlList += url
          // }
          val iter: util.Iterator[UrlWindowCount] = urls.iterator()
          while (iter.hasNext) {
            urlList += iter.next()
          }
          val sortedList: ListBuffer[UrlWindowCount] = urlList.sortWith((c1, c2) => {
            c1.count > c2.count
          }).take(3)

          // 清空状态
          valueListState.clear()
          timeServState.clear()

          // 打印控制台
          sb.append("时间：").append(new Timestamp(timestamp - 1000L)).append("\n")

          for (i <- sortedList.indices) {
            val currentUrlView: UrlWindowCount = sortedList(i)
            sb.append("No").append(i + 1).append(":")
              .append(" URL:").append(currentUrlView.url)
              .append(" 访问量:").append(currentUrlView.count).append("\n")
          }

          sb.append("==================\n")
          Thread.sleep(1000)
          out.collect(sb.toString)
        }
      })
      .print("top")

    env.execute("network")
  }
}
