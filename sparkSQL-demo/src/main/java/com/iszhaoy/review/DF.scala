package com.iszhaoy.review

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datanucleus.store.rdbms.sql.SQLJoin.JoinType

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/28 12:05
 * @version 1.0
 */

case class Person(name: String, age: Int)

object DF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("df").config(new SparkConf)
      .master("local[*]")
      .getOrCreate()

    // 从数据源直接读取
    //    val resourcePath: String = getClass.getClassLoader.getResource("people.json").getPath
    //    val jsonDataFrame: DataFrame = spark.read.json(resourcePath)
    //    jsonDataFrame.show()

    import spark.implicits._
    // rdd toDF转dataframe
    //    val rdd = spark.sparkContext.parallelize(List((1,"zhangsan"),(2,"lisi"),(3,"wangwu")))
    //    val userDataFrame: DataFrame = rdd.toDF(colNames = "id", "name")
    //    userDataFrame.show()
    //
    //    val ids = "2:3:5"
    //    val idSet: Seq[String] = ids.split(":").toSeq
    //    val idDataFrame: DataFrame = spark.sparkContext.parallelize(idSet, 2).toDF("id")
    //
    ////    userDataFrame.join(idDataFrame,$"id" === $"id","inner").show()
    //    idDataFrame.join(userDataFrame,Seq("id"),"left").show()

    // 通过样例类简历dataframe
    val path: String = this.getClass.getClassLoader.getResource("people.txt").getPath
    val peopleRDD: RDD[String] = spark.sparkContext.textFile(path)
    //    val peopleDataFrame: DataFrame = peopleRDD.map(line => {
    //      val dataArrs: Array[String] = line.split(",")
    //      Person(dataArrs(0).trim, if (dataArrs(1) != "") dataArrs(1).trim.toInt else 0)
    //    }).toDF()
    //    peopleDataFrame.show()

    val structType: StructType = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        Nil
    )
    val data: RDD[Row] = peopleRDD.map {
      line =>
        val dataArrs: Array[String] = line.split(",")
        Row(dataArrs(0), dataArrs(1).trim.toInt)
    }
    val DF: DataFrame = spark.createDataFrame(data, structType)

    // DF 2 RDD
    val rdd: RDD[Row] = DF.rdd
    // RDD 2 DF
    rdd.map(r => r.getAs[String]("name")).toDF("name").show()
    rdd.collect.foreach(println)

    spark.stop()
  }
}
