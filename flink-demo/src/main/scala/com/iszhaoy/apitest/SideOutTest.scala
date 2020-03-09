package com.iszhaoy.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
;

object SideOutTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("hadoop01", 9999)
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    val processedStream: DataStream[SensorReading] = dataStream
      .process(new FreezingAlter())

    //    dataStream.print("dataStream")
    processedStream.print("processedStream")
    processedStream.getSideOutput(new OutputTag[String]("freezing_alter")).print("alter")

    env.execute("processe function")
  }

}

// 冰点报警，如果小于32F,输出报警信息到侧输出流
class FreezingAlter() extends ProcessFunction[SensorReading, SensorReading] {

  lazy val alterOutput: OutputTag[String] = new OutputTag[String]("freezing_alter")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(alterOutput, "freezing alter for :" + value.id)
    } else {
      // 主输入流
      out.collect(value)
    }
  }
}