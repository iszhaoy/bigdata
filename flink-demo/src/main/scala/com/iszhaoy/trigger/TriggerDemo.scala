package com.iszhaoy.trigger

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorReadingStream = env.socketTextStream("localhost", 19999)
      .map(data => {
        val dataArrs = data.split(",")
        SensorReading(dataArrs(0).trim, dataArrs(1).trim.toLong, dataArrs(2).toDouble)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp * 1000
      }
    })

    val TriggerStream = sensorReadingStream.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(CountTrigger.of(2))
      .process(new ProcessWindowFunction[SensorReading, SensorReading, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[SensorReading]): Unit = {
          for (elem <- elements.iterator) {
            out.collect(elem)
          }
        }
      })
    sensorReadingStream.print("sensorReadingStream")
    TriggerStream.print("TriggerStream")

    env.execute("TriggerDemo")
  }
}
