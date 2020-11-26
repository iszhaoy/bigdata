package com.iszhaoy.join

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class LeftClass(key: String, value: String, datetime: Long)

case class RightClass(key: String, value: String, datetime: Long)

case class ResultClass(key: String, leftValue: String, value: String, windowTime: Long)

object JoinTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val leftLog = getClass.getResource("/testJoin/left.log").getPath
    val rightLog = getClass.getResource("/testJoin/right.log").getPath

    val leftStream = env.readTextFile(leftLog)
      .map(data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val arrs = data.split(",")
        LeftClass(arrs(0).trim, arrs(1).trim, sdf.parse(arrs(2)).getTime)
      }).assignAscendingTimestamps(_.datetime)

    val rightStream = env.readTextFile(rightLog)
      .map(data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val arrs = data.split(",")
        RightClass(arrs(0).trim, arrs(1).trim, sdf.parse(arrs(2)).getTime)
      }).assignAscendingTimestamps(_.datetime)
    // InnerJoin
    // val resultStream = leftStream.join(rightStream)
    //   .where(_.key).equalTo(_.key)
    //   .window(TumblingEventTimeWindows.of(Time.seconds(30)))
    //   .apply(new InnerJoinFunction())

    // val resultStream = leftStream.coGroup(rightStream)
    //   .where(_.key).equalTo(_.key)
    //   .window(TumblingEventTimeWindows.of(Time.seconds(30)))
    //   .apply(new LeftJoinFunction())
    //   .apply(new RightJoinFunction())

    val resultStream = leftStream
      .keyBy(_.key)
      .intervalJoin(rightStream.keyBy(_.key))
      .between(Time.seconds(-10), Time.seconds(20))
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new ProcessJoinFunction[LeftClass, RightClass, ResultClass]() {
        override def processElement(left: LeftClass, right: RightClass, ctx: ProcessJoinFunction[LeftClass, RightClass, ResultClass]#Context, out: Collector[ResultClass]): Unit = {
          out.collect(ResultClass(left.key, left.value, right.value, left.datetime))
        }
      })

    resultStream.print("test")
    env.execute("JoinTest")
  }
}

class InnerJoinFunction() extends JoinFunction[LeftClass, RightClass, ResultClass] {
  override def join(first: LeftClass, second: RightClass): ResultClass = {
    ResultClass(first.key, first.value, second.value, first.datetime)
  }
}


class LeftJoinFunction() extends CoGroupFunction[LeftClass, RightClass, ResultClass] {
  override def coGroup(first: lang.Iterable[LeftClass], second: lang.Iterable[RightClass], out: Collector[ResultClass]): Unit = {
    import scala.collection.JavaConversions._
    val leftList = first.toList
    val rightList = second.toList
    for (l <- leftList) {
      var isMatch = false
      for (r <- rightList) {
        out.collect(ResultClass(l.key, l.value, r.value, l.datetime))
        isMatch = true
      }
      if (!isMatch) {
        out.collect(ResultClass(l.key, l.value, null, l.datetime))
      }
    }
  }
}

class RightJoinFunction() extends CoGroupFunction[LeftClass, RightClass, ResultClass] {
  override def coGroup(first: lang.Iterable[LeftClass], second: lang.Iterable[RightClass], out: Collector[ResultClass]): Unit = {
    import scala.collection.JavaConversions._
    val leftList = first.toList
    val rightList = second.toList
    for (r <- rightList) {
      var isMatch = false
      for (l <- leftList) {
        out.collect(ResultClass(r.key, l.value, r.value, r.datetime))
        isMatch = true
      }
      if (!isMatch) {
        out.collect(ResultClass(r.key, null, r.value, r.datetime))
      }
    }
  }
}




