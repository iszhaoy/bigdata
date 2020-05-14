import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
/**
 * @author zhaoyu
 * @date 2020/5/8
 */

case class UvCount(weindowEnd: Long, uvCount: Long)
object UniqueVisitor {
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
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
          // 将窗口内的元素去重,并count
          val set: mutable.Set[Long] = new mutable.HashSet[Long]()
          for (u <- input) {
            set += u.userId
          }
          out.collect(UvCount(window.getEnd,set.size))
        }
      }).print("uv")

    env.execute("uv")
  }
}
