package com.iszhaoy.table

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

case class Person(id: String, Phone: String)

case class AggResult(id: String, nums: Long)

object ExplodeAndAgg {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val flatMapStream = env.fromCollection(Seq[Person](
      Person("1", "1399300248/13453857881"),
      Person("2", "1300300400"),
      Person("3", "1243212345/12345346788"),
      Person("4", "11011011011")
    )).flatMap(new MySelfFlatMapFunction)
    flatMapStream.print("Explode")

    //flatMapStream
    //  .map(p => (p.id, 1L))
    //  .keyBy(0)
    //  .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    //  .sum(1)
    //  .print()


    env.execute("test")

  }
}

class MySelfFlatMapFunction extends FlatMapFunction[Person,Person]  with ResultTypeQueryable[Person]{
  val log = LoggerFactory.getLogger(getClass)
  override def flatMap(person: Person, out: Collector[Person]): Unit = {
    val phones = person.Phone.split("/")
    log.debug(s"key : ${person.id} 下有 ${phones.size} 个手机号")
    for (p <- phones) {
      out.collect(Person(person.id, p))
    }
  }

  // 为输出类型提供TypeInformation
  override def getProducedType: TypeInformation[Person] = Types.CASE_CLASS[Person]
}