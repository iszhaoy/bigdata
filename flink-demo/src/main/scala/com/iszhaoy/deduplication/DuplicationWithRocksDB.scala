package com.iszhaoy.deduplication

import java.net.URL

import com.alibaba.fastjson.JSON
import com.iszhaoy.broadcast.Order
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/14 15:15
 * @version 1.0
 *
 */

object DuplicationWithRocksDB {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // start a checkpoint every 1000 ms// start a checkpoint every 1000 ms
    env.enableCheckpointing(5 * 60 * 1000)
    // advanced options:
    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // make sure 500 ms of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // enable externalized checkpoints which are retained after job cancellation
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val rocksDBStateBackend = new RocksDBStateBackend("file:///tmp/flink-2020-07-14", true)
    //rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    // 设置快照/恢复时用于传输文件的线程数。
    rocksDBStateBackend.setNumberOfTransferThreads(2)
    // 设置合并压缩时清除ttl，状态TTL仅对时间特征为处理时间时生效方法已经过时，因为默认启用
    rocksDBStateBackend.enableTtlCompactionFilter()
    env.setStateBackend(rocksDBStateBackend)

    val resource: URL = getClass.getResource("/order.log")
    env.readTextFile(resource.getPath)
      .filter(_ != null)
      .map(data => {
        JSON.parseObject(data, classOf[Order])
      })
      .assignAscendingTimestamps(_.time * 1000L)
      .keyBy(json => (json.userId, json.orderId))
      .process(new RocksDBDeDuplicationFunction())
      .print("Duplication")

    env.execute("DuplicationWithRocksDB")
  }
}

class RocksDBDeDuplicationFunction() extends KeyedProcessFunction[(String, String), Order, Order] {
  private var existState: ValueState[Boolean] = _


  override def open(parameters: Configuration): Unit = {
    // 设置状态TTL配置
    val stateTtlConfig: StateTtlConfig = StateTtlConfig
      // 过期时间设为1天
      .newBuilder(Time.days(1))
      // 已经过期的数据不能再被访问到
      .setStateVisibility(StateVisibility.NeverReturnExpired)
      // 在状态值被创建和被更新时重设TTL
      .setUpdateType(UpdateType.OnCreateAndWrite)
      // 在每处理10000条状态记录之后，更新检测过期的时间戳。这个参数要小心设定，更新太频繁会降低compaction的性能，更新过慢会使得compaction不及时，状态空间膨胀。
      .cleanupInRocksdbCompactFilter(10000)
      .build()

    // 获取状态描述器，并设置TTL配置
    val existStateDesc: ValueStateDescriptor[Boolean] = new ValueStateDescriptor[Boolean]("suborder-dedup-state",
      classOf[Boolean])
    existStateDesc.enableTimeToLive(stateTtlConfig)

    existState = getRuntimeContext.getState(existStateDesc)
  }

  override def processElement(value: Order, ctx: KeyedProcessFunction[(String, String), Order, Order]#Context,
                              out: Collector[Order]): Unit = {
    // 在实际处理数据时，如果数据的key（即userId + orderId）对应的状态不存在，说明它没有出现过，可以更新状态并输出。反之，说明它已经出现过了，直接丢弃
    if (existState.value == null) {
      existState.update(true)
      out.collect(value)
    }
    /*
    * PS:还需要注意一点，若数据的key占用的空间比较大（如长度可能会很长的字符串类型），也会造成状态膨胀。
    * 我们可以将它hash成整型再存储，这样每个key就最多只占用8个字节了。
    * 不过任何哈希算法都无法保证不产生冲突，所以还是得根据业务场景自行决定。
    * */
  }
}
