package com.iszhaoy.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

object ProcessWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorStream = env.socketTextStream("localhost", 9999)
      .map(data => {
        val dataArry: Array[String] = data.split(",")
        SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    //    sensorStream
    //      .keyBy(_.id)
    //      .timeWindow(Time.seconds(10))
    //      .process(new HightAndLowTempProcessFunction())
    sensorStream.map(x => (x.id, x.temperature, x.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      // 可以将增量聚合函数和处理窗口一起使用，这样才处理窗口时就拿到的只有一个值，减少消耗
      .reduce((r1: (String, Double, Double), r2: (String, Double, Double)) => {
        (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
      }, new ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
          val head = elements.head
          out.collect(MinMaxTemp(key, head._2, head._3, context.window.getEnd))
        }
      })
  }
}

class HightAndLowTempProcessFunction() extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
    val temps = elements.map(_.temperature)
    val max = temps.max
    val min = temps.min
    out.collect(MinMaxTemp(key, min, max, context.window.getEnd))
  }
}
