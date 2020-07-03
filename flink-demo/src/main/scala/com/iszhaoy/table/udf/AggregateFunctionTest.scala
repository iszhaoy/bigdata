package com.iszhaoy.table.udf

import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 18:21
 * @version 1.0
 */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("sernsor.txt").getPath
    val dataStream: DataStream[SensorReading] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      SensorReading(dataArry(0), dataArry(1).toLong, dataArry(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    val avgTemp = new AvgTemp()

    sensorTable
      .groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('avgTemp)
      .toRetractStream[Row]
      .filter(_._1 == true)
      .print()

    env.execute("job")
  }

  // 定义聚合函数的状态类，用户保存聚合状态（sum，count）
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  // 自定义一个球hashcode的标量函数
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {

    def accumulate(acc: AvgTempAcc, temp: Double) = {
      acc.sum += temp
      acc.count += 1
    }

    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc()
  }

}
