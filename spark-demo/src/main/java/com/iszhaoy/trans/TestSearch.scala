package com.iszhaoy.trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSearch {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val word: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d"))

    val search = new Search("a")

    val filtered: RDD[String] = search.getMatche2(word)

    filtered.foreach(println)

    sc.stop()

  }
}
