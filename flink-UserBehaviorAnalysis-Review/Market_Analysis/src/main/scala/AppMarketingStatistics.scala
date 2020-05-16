import java.sql.Timestamp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/5/15 16:50
 * @version 1.0
 */


object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.addSource(new MyEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(_ => ("dummay", 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(1))
      .process(new ProcessWindowFunction[(String, Long), MarketingViewCount, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Long)],
                             out: Collector[MarketingViewCount]): Unit = {
          val startTime = new Timestamp(context.window.getStart).toString
          val endTime = new Timestamp(context.window.getEnd).toString
          val count = elements.size

          out.collect(MarketingViewCount(startTime, endTime, "", "", count))
        }
      })


      .print("AppMarketingStatistics")

    env.execute("AppMarketingStatistics")

  }

}
