import java.lang

import akka.stream.stage.GraphStageLogic.StageActorRefNotInitializedException
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

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
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new Trigger[(String, Long), TimeWindow] {
        override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext)
        : TriggerResult = {
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
      }).process(new UvCountWithBloom())
      .print("bloom")

    env.execute("uv")

  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  // ProcessWindowFunction 是全窗口函数，先把窗口所有数据收集起来，等到计算的时候会遍历所有数据

  // 定义redis的连接
  lazy val jedis = new Jedis("hadoop01", 6379)
  lazy val bloom = new Bloom(1 << 29) // 512m   1024 * 1024 * 2 ^ 9

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式，key是windowEnd,value就是bitmap

    val storeKey = context.window.getEnd.toString
    var count = 0L
    // 把每个窗口的count值也存入redis表里（windowend ->count）,所以要先从redis中读取。

    val value: String = jedis.hget("count", storeKey)
    if (value != null) {
      count = value.toLong
    }

    // 使用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString // 拿出用户id

    val offset: Long = bloom.hash(userId, 61)
    // 定义一个标志位，判断redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      // 如果不存在，位置置为1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}

class Bloom(size: Long) extends Serializable {
  // 定义位图的总大小
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash function
  def hash(value: String, seed: Long): Long = {
    var result = 0L
    for (i <- Range(0, value.length)) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }

}