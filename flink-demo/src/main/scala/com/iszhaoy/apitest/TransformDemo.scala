package com.iszhaoy.apitest

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object TransformDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val streamFormFile: DataStream[String] = env.readTextFile("flink-demo\\src\\main\\resources\\sernsor.txt")

    val dataStream: DataStream[SensorReading] = streamFormFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // keyby reduce 聚合算子
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(x => x.id)
      //      .sum(2)
      // .print("sum")
      // 输出但该案的传感器最新的问题+10，而时间戳是上一次数据的时间+1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
    //      .print("reduce")


    // split 多流转换算子
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("hight") else Seq("low")
    })

    val hightTmpStream: DataStream[SensorReading] = splitStream.select("hight")
    val lowTmpStream: DataStream[SensorReading] = splitStream.select("low")
    val allTmpStream: DataStream[SensorReading] = splitStream.select("hight", "low")
    hightTmpStream.print("hight")
    lowTmpStream.print("low")
    allTmpStream.print("all")

    // 合并两条流的算子
    val warningStream: DataStream[(String, Double)] = hightTmpStream.map(data => (data.id, data.temperature))
    // collect算子
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTmpStream)
    // 返回的必须为tuple类型
    val coMapDataStream: DataStream[Product] = connectedStream.map(
      waringData => (waringData._1, waringData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapDataStream.print("coMapDataStream")

    // union合并多条流的算子
    val unionStream: DataStream[SensorReading] = hightTmpStream.union(lowTmpStream)
    unionStream.print("unionStream")

    // 函数类
    val filterDataStream: DataStream[SensorReading] = dataStream.filter(new MyFilter())
    filterDataStream.print("filterDataStream")


    env.execute("TransformDemo")

  }
}

// 自定义Filter
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}
