package com.iszhaoy.review

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/29 15:14
 * @version 1.0
 */

case class UserInfo(username: String, isMask: Boolean)

object FilterBlackList {
  val shutdownMarker = "hdfs://hadoop01:9000/tmp/spark-test/stop-spark"
  var stopFlag: Boolean = false

  def craeteSSC() = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("FilterBlackList")
    // 优雅关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(conf, Seconds(5))

    val deserializationClass: String = "org.apache.kafka.common.serialization.StringSerializer"
    val broker_list = "hadoop01:9092"
    val group_id = "iszhaoy001"
    val topic = "blacklist-2020-06-29"

    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list
    )
    val KafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam,
      Set(topic))

    val line: DStream[String] = KafkaDStream.map(_._2)
    line
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .transform(rdd => {
        val blacklist: ArrayBuffer[UserInfo] = Blacklist.getInstance(rdd.sparkContext).value
        val blackListRDD: RDD[(String, Boolean)] = rdd.sparkContext.parallelize(blacklist).map(x => (x.username, x.isMask))

        // 第二次重启会有问题
        val leftJoinRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blackListRDD)
        leftJoinRDD.filter {
          case (key, (_, isMask)) => {
            //            println(
            //              s"""
            //                 |${(key, isMask.getOrElse(false))}
            //                 |""".stripMargin)
            if (!isMask.getOrElse(false)) {
              true
            } else {
              println(
                s"""
                   | ${(key, isMask.get + "●")}
                   |""".stripMargin)
              false
            }
          }
        }.map("whiteList: " + _._1)
      }).print()
    ssc.checkpoint("/tmp/blackList-2020-06-29")
    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ssc: StreamingContext = StreamingContext.getOrCreate("/tmp/blackList-2020-06-29", () => craeteSSC())
    ssc.start()
    ssc.awaitTermination()
    //    //检查间隔毫秒
    //    val checkIntervalMillis = 10000
    //
    //    var isStopped = false
    //    while (!isStopped) {
    //      println("calling awaitTerminationOrTimeout")
    //      //等待执行停止。执行过程中发生的任何异常都会在此线程中抛出，如果执行停止了返回true，
    //      //线程等待超时长，当超过timeout时间后，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。
    //      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
    //      if (isStopped) {
    //        println("confirmed! The streaming context is stopped. Exiting application...")
    //      } else {
    //        println("Streaming App is still running. Timeout...")
    //      }
    //      //判断文件夹是否存在
    //      checkShutdownMarker
    //      if (!isStopped && stopFlag) {
    //        println("stopping ssc right now")
    //        //第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
    //        //第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止。
    //        ssc.stop(true, true)
    //        println("ssc is stopped!!!!!!!")
    //      }
    //    }
  }

  //  def checkShutdownMarker = {
  //    if (!stopFlag) {
  //      //开始检查hdfs是否有stop-spark文件夹
  //      val fs = FileSystem.get(new Configuration())
  //      //如果有返回true，如果没有返回false
  //      stopFlag = fs.exists(new Path(shutdownMarker))
  //    }
  //  }

}

/**
 * 在方法中初始化变量，在driver端拿到广播变量并在excutor端使用，便可实现广播变量的正常使用
 **/
object Blacklist {

  @volatile private var instance: Broadcast[ArrayBuffer[UserInfo]] = null

  // 实现初始化并更新广播变量
  def getInstance(sc: SparkContext): Broadcast[ArrayBuffer[UserInfo]] = {
    //查Redis获取配置信息写到数组arr...
    val list: ArrayBuffer[UserInfo] = new ArrayBuffer[UserInfo]()
    list.append(UserInfo("zs", false))
    list.append(UserInfo("ls", true))
    list.append(UserInfo("ww", false))
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(list)
        }
      }
    } else {
      synchronized {
        // 每次调用前会先unpersist释放再获取最新的配置值重新广播，实现了广播变量的动态更新。
        instance.unpersist();
        instance = sc.broadcast(list);
      }
    }
    instance
  }
}