
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author zhaoyu
 * @date 2020/5/7
 */


// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 加载数据
    val resourcePath: String = getClass.getClassLoader.getResource("UserBehavior.csv").getPath
    val resultDataStrem = env.readTextFile(resourcePath)
      .map(data => {
        //543462,1715,1464116,pv,1511658000
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt,
          dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 分配水位和时间戳(因为没有乱序，使用升序分配时间戳方法)
      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 过滤含有pv的数据
      .filter(_.behavior == "pv")
      // 根据itemid进行分组
      .keyBy(_.itemId)
      // 滚动窗口，窗口大小为1小时，每5分钟滑动一次
      .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
      // 对窗口内的元素聚合,统计
      .aggregate(new AggregateCount(), new WindowAggregate())
      // 对窗口进行分组
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    resultDataStrem.setParallelism(1).print("top n")
    env.execute("hot_items_analysis")
  }
}

class AggregateCount() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowAggregate() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopN(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 将窗口下的ItemViewCount都存入listState中，在窗口结束后延迟1秒将数据全部取出，排序，取top n
  lazy val valueListState: ListState[ItemViewCount] = getRuntimeContext
    .getListState(new ListStateDescriptor[ItemViewCount]("item", classOf[ItemViewCount]))

  lazy val timeValueState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeserv", classOf[Long]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

    valueListState.add(value)
    if (timeValueState.value() == 0) {
      // 如果没有定时器，则注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1000L)
      timeValueState.update(1L)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 参数中的timestamp 是触发定时器时候的时间

    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- valueListState.get()) {
      allItems.append(item)
    }

    val sortItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(n)

    // 清空状态
    valueListState.clear()
    timeValueState.clear()

    val result: StringBuilder = new StringBuilder

    result.append("时间，").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortItems.indices) {
      val itemViewCount: ItemViewCount = sortItems(i)
      result.append(i + 1).append(":")
        .append("商品Id=").append(itemViewCount.itemId)
        .append(",浏览量=").append(itemViewCount.count)
        .append("\n")
    }
    result.append("==========================\n")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}