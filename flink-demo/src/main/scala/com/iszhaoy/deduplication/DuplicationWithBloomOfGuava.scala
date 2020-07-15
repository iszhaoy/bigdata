package com.iszhaoy.deduplication

import java.nio.charset.Charset

import com.alibaba.fastjson.JSON
import com.google.common.hash.{BloomFilter, Funnels}
import com.iszhaoy.joindim.Order
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/11 16:00
 * @version 1.0
 *
 *          demo实现了使用Guava的BloomFilter对数据实时去重，并在第二天0点充值BloomFilter
 *          当然 使用BloomFilter会有所偏差
 *          因为BloomFilter的特性就是一个概率性的：一定不存在或者可能存在
 */

object DuplicationWithBloomOfGuava {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val resource = getClass.getResource("/order.log")
    env.readTextFile(resource.getPath)
      .filter(_ != null)
      .map(data => {
        JSON.parseObject(data, classOf[Order])
      })
      .assignAscendingTimestamps(_.time * 1000L)
      .keyBy(_.userId)
      .process(new DeduplicateOrderProcessFunction())
      .name("process_uv_day").uid("process_uv_day")
      .print("duplication")
    env.execute("DuplicationWithBloomOfGuava")
  }
}

class DeduplicateOrderProcessFunction() extends KeyedProcessFunction[String, Order, Order] {
  // 布隆过滤器的期望最大数据量应该按每天产生子订单最多的那个站点来设置，这里设为100万
  // 可容忍的误判率为1%
  // 单个布隆过滤器需要8个哈希函数，其位图占用内存约114MB，压力不大
  private val BF_CARDINAL_THRESHOLD: Int = 1000000
  private val BF_FALSE_POSITIVE_RATE: Double = 0.01
  @volatile var userBehaviorFilterState: ValueState[BloomFilter[String]] = _

  override def open(parameters: Configuration): Unit = {
    val s: Long = System.currentTimeMillis

    userBehaviorFilterState = getRuntimeContext.getState(new
        ValueStateDescriptor[BloomFilter[String]]("filter", TypeInformation.of(new TypeHint[BloomFilter[String]] {})))

    val e: Long = System.currentTimeMillis
    println("Created Guava BloomFilter, time cost: " + (e - s))
  }

  override def processElement(value: Order, ctx: KeyedProcessFunction[String, Order,
    Order]#Context, out: Collector[Order]): Unit = {
    var userBehaviorFilter: BloomFilter[String] = userBehaviorFilterState.value()

    if (userBehaviorFilter == null) {
      // 创建布隆过滤器,初始化
      userBehaviorFilter = BloomFilter.create[String](
        Funnels.stringFunnel(Charset.forName("utf-8")), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE)
    }

    val userId: String = value.userId
    val orderId: String = value.orderId

    // 如果userId和orderId相同，说明重复
    if (!userBehaviorFilter.mightContain(s"${userId}_${orderId}")) {
      userBehaviorFilter.put(s"${userId}_${orderId}")
      out.collect(value)
    }

    // 更新状态
    userBehaviorFilterState.update(userBehaviorFilter)

    ctx.timerService().registerProcessingTimeTimer(getTomorowZeroTimestampMS(ctx.timerService().currentProcessingTime(),
      8) + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Order, Order]#OnTimerContext,
                       out: Collector[Order]): Unit = {
    val s: Long = System.currentTimeMillis

    // 每日零点创建布隆过滤器
    // 清空状态
    userBehaviorFilterState.clear()
    // 重置布隆过滤器
    userBehaviorFilterState.update(BloomFilter.create[String](
      Funnels.stringFunnel(Charset.forName("utf-8")), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE))

    val e: Long = System.currentTimeMillis
    println("Timer triggered & resetted Guava BloomFilter, time cost: " + (e - s))
  }

  override def close(): Unit = {
    userBehaviorFilterState = null
  }

  def getTomorowZeroTimestampMS(now: Long, timeZone: Int): Long = {
    now - (now + timeZone * 3600000) % 86400000 + 86400000
  }
}

