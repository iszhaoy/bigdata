import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author zhaoyu
 * @date 2020/5/16
 */


case class AdClickLog(userId: Long, adId: Long, province: String, city: String,
                      timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

object AdStatisticsByGeo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile(getClass.getResource("AdClickLog.csv").getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickLog(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim,
          dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 根据省份分区
      .keyBy(_.province)
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
      //      .process(new MyProcessWindowsFunction)
      .aggregate(new MyAggCount, new MyWindowResult)
      .print("count by prov")

    env.execute("count by prov")
  }
}

class MyProcessWindowsFunction extends ProcessWindowFunction[AdClickLog, CountByProvince, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AdClickLog], out: Collector[CountByProvince]): Unit = {
    val endTime: String = new Timestamp(context.window.getEnd).toString
    val count: Int = elements.size
    out.collect(CountByProvince(endTime, key, count))
  }
}

class MyAggCount extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class MyWindowResult extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(formatDate(window.getEnd), key, input.iterator.next()))
  }

  def formatDate(date: Long): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
    sdf.format(new Date(date))
  }
}