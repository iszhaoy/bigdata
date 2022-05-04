package test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TaskManagerNumsTest {
  def main(args: Array[String]): Unit = {
    //val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(2)

    env.socketTextStream("bigdata01", 9999)
      .flatMap(data => data.split(" "))
      .setParallelism(8)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("test")

    env.execute("TaskManagerNumsTest")

  }
}
