package com.iszhaoy.hive

import org.apache.spark.sql.SparkSession

object TestHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("show tables").show()

    spark.stop()
  }
}
