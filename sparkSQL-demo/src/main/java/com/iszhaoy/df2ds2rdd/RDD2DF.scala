package com.iszhaoy.df2ds2rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DF {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("wordcount")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // 创建RDD
    val rdd: RDD[Int] = spark
      .sparkContext.parallelize(Array(1, 2, 3, 4))

    // 将RDD[Int]转换为RDD[Row]
    val rowRDD: RDD[Row] = rdd.map(x => {
      Row(x)
    })

    // 创建结构信息
    val structType: StructType = StructType(StructField("id", IntegerType) :: Nil)

    // 创建DF
    val df: DataFrame = spark
      .createDataFrame(rowRDD, structType)

    df.show()


  }
}
