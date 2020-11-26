package com.iszhaoy.table.udf

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/3 10:47
 * @version 1.0
 */

case class ExplodeClass(id: String,name:String, phone: String)

object TableFunctionTest2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("explode/test.log").getPath
    val dataStream: DataStream[ExplodeClass] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      ExplodeClass(dataArry(0), dataArry(1).trim,dataArry(2).trim)
    })

    val ExplodeClassTable: Table = tableEnv.fromDataStream(dataStream, 'id,'name, 'phone)

    val split = new Split("/")

    tableEnv.createTemporaryView("ExplodeClassTable", ExplodeClassTable)
    tableEnv.registerFunction("split", split)
    tableEnv.sqlQuery(
      """
        | select id,name,temp.p
        | from
        | ExplodeClassTable,lateral table(split(phone)) as temp(p)
        |""".stripMargin
    ).toAppendStream[Row].print("sql")


    env.execute("job")

  }

  class Split(sep: String) extends TableFunction[String] {
    def eval(str: String): Unit = {
      str.split(sep).foreach(
        (p: String) => collect(p)
      )
    }
  }

}
