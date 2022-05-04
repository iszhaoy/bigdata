package com.iszhaoy.hive

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession

import java.net.URL

object TestHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()

    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

    //spark.sql("create function my_udf as 'com.iszhaoy.udf.StringUtilsUDF' using jar 'hdfs:/udf/hive-function-1.0-SNAPSHOT.jar'")
    spark.sql("select my_udf_test('ABC')").show(false)

    spark.stop()
  }
}
