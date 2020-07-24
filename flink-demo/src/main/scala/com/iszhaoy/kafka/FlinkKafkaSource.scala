package com.iszhaoy.kafka

import java.util.Properties

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.{RocksDBOptions, RocksDBStateBackend}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.TernaryBoolean

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/6 9:53
 * @version 1.0
 */
object FlinkKafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "hadoop01:9092")
    props.setProperty("group.id", "iszhaoy002")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //props.setProperty("auto.offset.reset", "latest")
    //props.setProperty("auto.offset.reset", "earliest")

    val sourceTopic = "flinksourcekafka-2020-07-06"
    val sinkTopic = "flinksourcekafka-2020-07-06_out"
    val consumer = new FlinkKafkaConsumer011[String](sourceTopic, new SimpleStringSchema(), props)
    //默认读取上次保存的offset信息
    //如果是应用第一次启动，读取不到上次的offset信息，则会根据这个参数auto.offset.reset的值来进行消费数据
    //consumer.setStartFromGroupOffsets()

    //从最早的数据开始进行消费，忽略存储的offset信息
    //consumer.setStartFromEarliest()

    //从最新的数据进行消费，忽略存储的offset信息
    //consumer.setStartFromLatest()

    //Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
    consumer.setStartFromTimestamp(1593916371000L)

    //从指定位置进行消费
    //val offsets = new util.HashMap[KafkaTopicPartition, lang.Long]()
    //offsets.put(new KafkaTopicPartition(topic, 0), 0L);
    //offsets.put(new KafkaTopicPartition(topic, 1), 0L);
    //consumer.setStartFromSpecificOffsets(offsets)

    // 使用checkpoint,每5秒保存一次
    env.enableCheckpointing(5000)
    val checkPointPath = new Path("file:///tmp/flink-checkpoint-2020-07-07")

    // 配置FsStateBackend
    val fsStateBackend = new FsStateBackend(checkPointPath)
    //env.setStateBackend(fsStateBackend)

    // 配置RocksDBbackend
    //val tmpDir = "D:/tmp/rocksdb/data"
    val rocksDBBackend: RocksDBStateBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE)
    val config = new Configuration()
    //TIMER分为HEAP(默认，性能更好)和RocksDB(扩展好)
    config.setString(RocksDBOptions.TIMER_SERVICE_FACTORY,RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString)
    rocksDBBackend.configure(config,getClass.getClassLoader)
    //rocksDBBackend.setDbStoragePath(tmpDir)
    env.setStateBackend(rocksDBBackend.asInstanceOf[StateBackend])

    // 设置模式为 exactly-once (default)
    // 一般对于超低延迟的应用(大概几毫秒)可以使用CheckpointingMode.AT_LEAST_ONCE，其他大部分应用使用CheckpointingMode.EXACTLY_ONCE就可以
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 检查点必须在一分钟内完成，否则将被丢弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 确保在检查点之间有500毫秒的进度
    // 用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，当指定这个参数时，maxConcurrentCheckpoints的值为1
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 同一时间只允许一个检查点执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 任务被取消之后，保留ck, 也可以配置删除ck
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置可容忍的检查点次数，如果检查点错误，任务会失败
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)

    val kafkaSource: DataStream[String] = env.addSource(consumer)
    val dataStream: DataStream[String] =
      kafkaSource.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
          .toString
    })

    val outprop: Properties = new Properties()
    outprop.setProperty("bootstrap.servers", "hadoop01:9092")
    //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
    //设置事务超时时间
    outprop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    //第二种解决方案，设置kafka的最大事务超时时间

    val kafkaProducer = new FlinkKafkaProducer011[String](
      sinkTopic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema),
      outprop)

    dataStream.addSink(kafkaProducer)

    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop01:9092",sinkTopic,new SimpleStringSchema()))


    env.execute("job")
  }
}
