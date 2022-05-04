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

case class ExplodeClass(phone: String)

object TableFunctionTest2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("explode/test1.log").getPath
    val dataStream: DataStream[ExplodeClass] = env.readTextFile(path).map(data => {
      val dataArry: Array[String] = data.split(",")
      ExplodeClass(dataArry(0).trim)
    })

    val ExplodeClassTable: Table = tableEnv.fromDataStream(dataStream, 'data)

    val split = new Split()

    tableEnv.createTemporaryView("temp_df", ExplodeClassTable)
    tableEnv.registerFunction("explode", split)
    tableEnv.sqlQuery(
      """
        | select T.data
        | from
        | temp_df,lateral table(explode(data,'/')) as T(data)
        |""".stripMargin
    ).toAppendStream[Row].print("sql")

    env.execute("job")

  }

  class Split extends TableFunction[String] {
    def eval(str: String,sep : String): Unit = {
      str.split(sep).foreach(
        (p: String) => collect(p)
      )
    }
  }

}
