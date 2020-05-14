import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author zhaoyu
 * @date 2020/5/9
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile(getClass.getResource("UserBehavior.csv").getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt,
          dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .map(_ => ("dummyKey", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new Trigger[(String, Int), TimeWindow] {
        override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.FIRE_AND_PURGE
        }

        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        }
      }).process(new ProcessWindowFunction[(String, Int), UvCount, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[UvCount]): Unit = {

      }
    })

  }
}
