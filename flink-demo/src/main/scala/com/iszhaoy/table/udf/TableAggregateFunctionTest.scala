package com.iszhaoy.table.udf


import com.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/2 18:21
 * @version 1.0
 */
object TableAggregateFunctionTest {
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

    val top2Temp = new Top2Temp()

    sensorTable
        .groupBy('id)
        .flatAggregate(top2Temp('temperature) as ('temp, 'rank))
        .select('id,'temp,'rank)
        .toRetractStream[Row]
        .print("api")


    env.execute("job")
  }

  // 定义聚合函数的状态类，用户保存聚合状态（sum，count）
  class Top2TempACC {
    var first: Double = Double.MinValue
    var sconds: Double = Double.MinValue
  }

  // 自定义一个球hashcode的标量函数
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempACC] {
    // 初始化状态
    override def createAccumulator(): Top2TempACC = new Top2TempACC()

    // 每来一个数据后，聚合计算的操作
    def accumulate(acc: Top2TempACC, temp: Double) = {
      // 判断您当前温度值，跟状态中的最高温和第二高温比较，如果大的话就替换
      if(temp > acc.first) {
        acc.sconds = acc.first
        acc.first = temp;
      } else if (temp > acc.sconds) {
        acc.sconds = temp
      }
    }

    // 实现一个输出数据的方法，写入结果表彰
    def emitValue(acc:Top2TempACC,out:Collector[(Double,Int)]) ={
      out.collect((acc.first,1))
      out.collect((acc.sconds,2))
    }

  }

}
