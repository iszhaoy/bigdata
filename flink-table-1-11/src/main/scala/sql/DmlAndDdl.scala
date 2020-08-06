package sql

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/17 9:53
 * @version 1.0
 */
object DmlAndDdl {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode()
      .useBlinkPlanner().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)


    //val dataStream: DataStream[SensorReading] = env.readTextFile(getClass.getResource("/sensor.txt").getPath)
    //  .map(data => {
    //    val dataArrs: Array[String] = data.split(",")
    //    SensorReading(dataArrs(0).trim, dataArrs(1).trim.toLong, dataArrs(2).trim.toDouble)
    //  })
    //val sensorTable: Table = tableEnv.fromDataStream(dataStream)

    //tableEnv.connect(new FileSystem().path(getClass.getResource("sensor.txt").getPath))
    //    .withFormat(new Csv().fieldDelimiter(','))
    //    .withSchema(
    //      new Schema()
    //        .field("id",DataTypes.STRING())
    //        .field("timestamp",DataTypes.BIGINT())
    //        .field("temperature",DataTypes.DOUBLE())
    //    ).createTemporaryTable("sensorTable")
    //tableEnv.sqlQuery(s"select * from sensorTable")
    //  .toAppendStream[SensorReading]
    //  .print("sensorTable")

    //val tableResult: TableResult = tableEnv.executeSql("select * from sensorTable")
    //tableResult.print()
    //val iter: CloseableIterator[Row] = tableResult.collect()
    //try while(iter.hasNext) {
    //  val row = iter.next()
    //  println(row)
    //}
    //finally iter.close()


    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("goodsTopic")
      .property("zookeeper.connect", "hadoop01:2181")
      .property("bootstrap.servers", "hadoop01:9092")
      .startFromLatest()
    )
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("goodsId", DataTypes.BIGINT())
          .field("goodsName", DataTypes.STRING())
          .field("isRemove", DataTypes.BOOLEAN())
      ).createTemporaryTable("goodsTable")

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("orderTopic")
      .property("zookeeper.connect", "hadoop01:2181")
      .property("bootstrap.servers", "hadoop01:9092")
      .startFromLatest()
    )
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("time", DataTypes.BIGINT())
          .field("orderId", DataTypes.STRING())
          .field("userId", DataTypes.STRING())
          .field("goodsId", DataTypes.BIGINT())
          .field("price", DataTypes.DOUBLE())
          .field("cityId", DataTypes.BIGINT())
      )
      .createTemporaryTable("orderTable")

    val joinTable: Table = tableEnv
      .sqlQuery(
        """
          |select *
          | from
          |   orderTable o
          | left join
          |   (select goodsId,goodsName from goodsTable where `isRemove` = false) g
          | on o.`goodsId` = g.`goodsId`
          |""".stripMargin)

    joinTable.toRetractStream[Row]
      .print("joinTable")
    env.execute("DmlAndDdl")
  }
}
