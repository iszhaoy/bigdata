package com.iszhaoy.hotitem_analysis


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/10 15:23
 * @version 1.0
 */


// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    val dataStream: DataStream[UserBehavior] = env
            .readTextFile("flink-UserBehaviorAnalysis/HotItemsAnylysis/src/main/resources/UserBehavior.csv")
//      .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim,
          dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合
      .keyBy(_.windowEnd) // 按照窗口分组
      .process(new TopNHotItems(3))

    // 4. sink控制态输出
    processedStream.print()
    env.execute("hot items job")
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp,
    accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, b._2 + b._2)
}

// 自定义窗口函数计算输出ItemCount
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //    val itemId:Long = key.asInstanceOf[Tuple1[Long]].f0
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  // 声明状态
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // classOf[ItemViewCount] 声明flink如何做序列化
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {

    // 把每条数据存入状态列表
    itemState.add(value)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  // 定时器触发时对素有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
    ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 将所有state中的数据取出，放到List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

    import scala.collection.JavaConversions._

    for (item <- itemState.get()) {
      allItems += item
    }

    // 按照count的大小排序,并取前n个
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间，").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每一个商品的信息
    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append("商品Id=").append(currentItem.itemId)
        .append(",浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("========================\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}