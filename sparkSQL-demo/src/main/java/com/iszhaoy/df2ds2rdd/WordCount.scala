package com.iszhaoy.df2ds2rdd

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount {

  def main(args: Array[String]): Unit = {

    // 1.创建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("wordcount")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val word: DataFrame = spark.read
      .textFile("sparkSQL-demo/src/main/data/wordcount.txt")
      .flatMap(_.split(" "))
      .map(x => WordCount(x.toString)).toDF("word")

    word.show(20)

    word.select($"word").show()
    println("**************")

    word.createOrReplaceTempView("word_table")
    spark
      .sql(
        """
          |select word,count(*) from word_table group by word
          |""".stripMargin)
        .show(false)
    println("**************")

    // 转化为RDD
    word.rdd.foreach(println)
    println("**************")

    // 转化为DS
    val wordDS: Dataset[WordCount] = word.as[WordCount]
    wordDS.show(false)
    println("**************")

    spark.stop
  }
}

case class WordCount(word:String) ()
