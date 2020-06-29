package com.iszhaoy.review

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/28 14:58
 * @version 1.0
 */
object DS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config(new SparkConf).getOrCreate()
    import spark.implicits._

    //    val DS: Dataset[Person] = Seq(Person("zhangsan", 11), Person("lisi", 22)).toDS()
    //    DS.show()

    val RDD: RDD[String] = spark.sparkContext.textFile(this.getClass.getClassLoader.getResource("people.txt").getPath)
    val DF2: Dataset[Person] = RDD.map(line => {
      val dataArrs: Array[String] = line.split(",")
      Person(dataArrs(0).trim, dataArrs(1).trim.toInt)
    }).toDS()
    DF2.show()

    val DF: DataFrame = DF2.toDF()
    DF.show()

    val DS2: Dataset[Person] = DF.as[Person]
    DS2.show()

    spark.stop()

  }
}
