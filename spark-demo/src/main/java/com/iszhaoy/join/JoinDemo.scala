package com.iszhaoy.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/6/28 10:07
 * @version 1.0
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("acc")

    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val leftRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "one"), (2, "two"), (3, "three")), 2)
    val rightRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "一"), (2, "二"), (4, "四")), 2)

    val joinRDD: RDD[(Int, (String, String))] = leftRDD.join(rightRDD)
    joinRDD.collect()
      .foreach(println)
    val leftJoinRDD: RDD[(Int, (String, Option[String]))] = leftRDD.leftOuterJoin(rightRDD)

    leftJoinRDD.map {
      case (key, (en, zh)) => (key, en, zh.orNull)
    }.collect().foreach(println)


    val rightJoinRDD: RDD[(Int, (Option[String], String))] = leftRDD.rightOuterJoin(rightRDD)

    rightJoinRDD.map {
      case (key, (en, zh)) => (key, en.orNull, zh)
    }.collect().foreach(println)

    leftRDD.fullOuterJoin(rightRDD).map {
      case (key, (en, zh)) => (key, en.orNull, zh.orNull)
    }
      .collect().foreach(println)
    sc.stop()

  }
}