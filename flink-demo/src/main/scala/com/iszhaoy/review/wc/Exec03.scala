package com.iszhaoy.review.wc


import com.iszhaoy.apitest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 小于32的放入侧输出流
 *
 * @author zhaoyu
 * @date 2020/5/6
 */
object Exec03 {
  val sideOutTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("sideoutput")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val filterDataStream: DataStream[SensorReading] = env.socketTextStream("bigdata02", 9999)
      .map(x => {
        try {
          val dataArry: Array[String] = x.split(",")
          SensorReading(dataArry(0).trim, dataArry(1).trim.toLong, dataArry(2).trim.toDouble)
        } catch {
          case e: Exception => {
            println("data format error!")
          }
            SensorReading("", 0L, 0.0)
        }
      })
      .filter(_.id != "")
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val processDataStream: DataStream[SensorReading] = filterDataStream
      .keyBy(_.id)
      .process(new MySideProcessFunction(32.0))

    processDataStream.print("s")
    processDataStream.getSideOutput(sideOutTag).print("sideOutTag")

    env.execute("exec 03")
  }

  class MySideProcessFunction(threshold: Double) extends KeyedProcessFunction[String,SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < threshold) {
        ctx.output(sideOutTag, value)
      } else {
        out.collect(SensorReading(value.id + "-s", value.timestamp, value.temperature))
      }
    }
  }

}


