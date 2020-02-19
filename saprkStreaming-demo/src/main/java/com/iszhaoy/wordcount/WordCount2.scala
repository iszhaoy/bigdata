package com.iszhaoy.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {

    // 初始spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")



    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    // 3. 通过监听端口穿件Dstream 读进来的数据为一行
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)

    val schema =
      StructType(
        StructField("word", StringType, true) ::
          StructField("ont", IntegerType, true) ::
         Nil)


    val resultDStream: DStream[Row] = lineStream.transform(rdd => {
      //      // 压平
      //      val wordcount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      //      wordcount
      val spark: SparkSession = SparkSession
        .builder()
        .config(rdd.sparkContext.getConf)
        .getOrCreate()

      import spark.implicits._

      val df: DataFrame = rdd
        .flatMap(_.split(" "))
        .map(x => Word(x))
        .toDF("word")

      df.createOrReplaceTempView("wordtable")
      val result: DataFrame = spark.sql(
        """
          |select word,count(*) from wordtable group by word
          |""".stripMargin)
      result.rdd
    })

    //打印
    resultDStream.print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Word(word: String)
