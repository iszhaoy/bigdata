package com.iszhaoy.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object ProcessTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    env.setStateBackend(new MemoryStateBackend())
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(1000000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    // job 失败后自动阐释重启3次，每次间隔500毫秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500))


    val stream: DataStream[String] = env.socketTextStream("bigdata02", 9999)
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    //    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
    //      .process(new TimeIncreAlert())

    val processedStream2: DataStream[(String, Double, Double)] = dataStream
      //      .flatMap(new TimechangeAlert2(10.0))
      .keyBy(_.id)
      .process(new TimechangeAlert(10.0))

    dataStream.print("dataStream")
    //    processedStream.print("processedStream")
    processedStream2.print("processedStream2")
    env.execute("processe function")
  }

}

class TimechangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lasttempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lasttempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",
      classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[
    (String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp: Double = lasttempState.value()

    // 用当前的问题之和上次的求差， 如果大于阈值就报警
    val diff: Double = (value.temperature - lastTemp).abs

    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lasttempState.update(value.temperature)
  }
}

class TimechangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量，保存上次的温度值
  lazy val lasttempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",
    classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double,
    Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {

    // 获取上次的温度值
    val lastTemp: Double = lasttempState.value()

    // 用当前的温度之和上次的求差， 如果大于阈值就报警
    val diff: Double = (value.temperature - lastTemp).abs

    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lasttempState.update(value.temperature)
  }
}


// process function
class TimeIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",
    classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTemp",
    classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    // 每次来一个数据先取出上一个温度值
    val preTemp: Double = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temperature)

    // 从状态中拿出定时器时间
    val currentTimerTs: Long = currentTimer.value()

    // 温度连续上升且没有注册定时器，则注册定时器
    if (value.temperature > preTemp && currentTimerTs == 0) {
      val timeTs: Long = ctx.timerService().currentProcessingTime() + 10000L
      // registerProcessingTimeTimer 时间是从1970-01-01 00:00:00 开始的
      ctx.timerService().registerProcessingTimeTimer(timeTs)
      currentTimer.update(timeTs)
    } else if (preTemp > value.temperature || preTemp == 0) {
      ctx.timerService().deleteProcessingTimeTimer(currentTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 直接报警信息
    out.collect("sensor_" + ctx.getCurrentKey + " 温度联系上升！！")
    currentTimer.clear()
  }
}