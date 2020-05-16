import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/5/15 12:19
 * @version 1.0
 */

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.addSource(new MyEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1) // 以渠道和行为类型为key
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
        override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)],
                             out: Collector[MarketingViewCount]): Unit = {
          val startTime = new Timestamp(context.window.getStart).toString
          val endTime = new Timestamp(context.window.getEnd).toString
          val channel = key._1
          val behavior = key._2
          val count = elements.size
          out.collect(MarketingViewCount(startTime, endTime, channel, behavior, count))
        }
      })
      .print("market_by_channel")

    env.execute("AppMarketByChannel")
  }
}

class MyEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  private var running = true

  val channelSet: Seq[String] = Seq("AppStore",
    "XiaomiStore",
    "HuaweiStore",
    "weibo",
    "wechat",
    "tieba")

  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
