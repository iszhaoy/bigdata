import java.sql.{Connection, PreparedStatement}
import java.util

import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/30 11:55
 * @version 1.0
 */
object KafkaStreamCommitOffsets {
  val group_id = "iszhaoy002"
  val topics = Array("offset-2020-06-30")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop01:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> group_id,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ssc: StreamingContext = createSSC()
    ssc.start()
    ssc.awaitTermination()
  }

  def createSSC() = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaStreamCommitOffsets").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(4))


    val fromOffsets: Map[TopicPartition, Long] = getMysqlOffsets(spark, null, topics(0))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )

    stream.map(_.value()).transform(rdd => {
      rdd.flatMap(_.split("\\s+"))
        .map((_, 1))
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(4), Seconds(4))
      .print()
    stream
      .foreachRDD {
        rdd => {
          // 查看分区offset
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.foreachPartition(iter => {
            saveMysqlOffsets(offsetRanges)
          })
        }
      }
    ssc
  }

  def saveMysqlOffsets(offsetRanges: Array[OffsetRange]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/cumin", "root", "root")
      conn.setAutoCommit(false)
      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId())
      ps = conn.prepareStatement(
        s"""
           |INSERT INTO OFFSETS (KAFKA_GROUP, TOPIC, PARTITION, OFFSET, UNTILOFFSET)
           | VALUES ('${group_id}','${o.topic}',${o.partition}, ${o.fromOffset},${o.untilOffset})
           |  ON DUPLICATE KEY UPDATE
           |  KAFKA_GROUP = '${group_id}',
           |  TOPIC = '${o.topic}',
           |  PARTITION =  ${o.partition},
           |  OFFSET =${o.fromOffset} ,
           |  UNTILOFFSET = ${o.untilOffset}
           |""".stripMargin)
      ps.executeUpdate()
      conn.commit()
      println(s"提交mysql成功 ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    } catch {
      case e: Exception => {
        // 提交出错 回滚
        println("提交失败，回滚", e)
        conn.rollback()
      }
    } finally {
      ps.close()
      conn.close()
    }
  }

  def getMysqlOffsets(spark: SparkSession, params: Map[String, String], topic: String): Map[TopicPartition, Long] = {
    import spark.implicits._
    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/cumin")
      .option("dbtable", "offsets")
      .option("user", "root")
      .option("password", "root")
      .load()
    val topicDF: Dataset[Row] = jdbcDF.where($"topic" === topic && $"kafka_group" === group_id)
    if (topicDF.count() != 0) {
      val fromOffsets: Map[TopicPartition, Long] = topicDF.rdd.map { row =>
        new TopicPartition(row.getAs[String]("topic"), row.getAs[Int]("partition")) -> row.getAs[Long]("offset")
      }.collect().toMap
      println("from offsets " + fromOffsets)
      fromOffsets
    } else {
      val util = new ZookeeperUtil(null)
      import scala.collection.JavaConverters._
      val partitions = util.getChild(ZkUtils.getTopicPartitionsPath(topic))
      val fromOffsets = scala.collection.mutable.Map[TopicPartition, Long]()
      for (p <- partitions.asScala) {
        println("初始化分区" + p)
        fromOffsets += (new TopicPartition(topic, p.toInt) -> 0L)
      }
      fromOffsets.toMap
    }
  }
}
