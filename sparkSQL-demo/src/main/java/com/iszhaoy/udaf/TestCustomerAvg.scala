package com.iszhaoy.udaf

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestCustomerAvg {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("wordcount")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("sparkSQL-demo/src/main/data/people.json")

    // 创建临时表
    df.createOrReplaceTempView("people")

    // 注册自定义函数
    spark.udf.register("MyAvg",CustomerAvg)

    // 使用
    spark.sql("select MyAvg(age) from people").show()

    spark.stop()

  }
}
