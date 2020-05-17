import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author zhaoyu
 * @date 2020/5/17
 */

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeoBlack {

  val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val adClickLogDataStream: DataStream[AdClickLog] = env.readTextFile(getClass.getResource("AdClickLog.csv").getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickLog(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim,
          dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    val filterBlackListStream: DataStream[AdClickLog] = adClickLogDataStream
      .keyBy(data => (data.userId, data.adId))
      .process(new BlackProcessFunction(100))


    filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AggregateFunction[AdClickLog, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: AdClickLog, acc: Long): Long = acc + 1L

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
      }, new WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
          out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
        }
      })
      .print("count")

    filterBlackListStream.getSideOutput(blackListOutputTag).print("balck")

    env.execute("balck exec")

  }

  class BlackProcessFunction(threshold: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
    lazy val countState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count", classOf[Int]))
    lazy val resetTimes: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimes", classOf[Long]))
    lazy val firstSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstSent", classOf[Boolean]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
      // 获取计数状态
      var curCount: Int = countState.value()
      if (curCount == 0) {
        // 如果是第一次处理，注册一个定时器，每天00:00 触发清除
        // 计算第二天的零时
        val ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * 24 * 60 * 60 * 1000
        resetTimes.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      if (curCount > threshold) {
        if (!firstSent.value()) {
          firstSent.update(true)
          // 如果超过阈值 ,报警
          // ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 1000L)
          // 将黑名单输出测流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + threshold + " times today."))
        }
        return
      }
      countState.update(curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
      if (timestamp == resetTimes.value()) {
        firstSent.clear()
        countState.clear()
      }
    }
  }

}


