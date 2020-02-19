package com.iszhaoy.parctice

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Agent {
  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("Agent").setMaster("local[*]")

    // 2.创建SparkConf
    val sc = new SparkContext(conf)

    // 3.读取文件 时间戳（TS，省份，城市，用户，广告
    val line: RDD[String] = sc.textFile("spark-demo/src/main/data/agent.log")

    // 3.提取省份和广告字段 (（PRO,ad)，1)
    val proAndADToOne: RDD[((String, String), Int)] = line.map(x => {
      val fields: Array[String] = x.split("\\s+")
      ((fields(1), (fields(4))), 1)
    })

    // 5. 求出每个省份每个广告么点击的总次数
    val proAndADCount: RDD[((String, String), Int)] = proAndADToOne.reduceByKey(_ + _)

    // 6. 扩大粒度
    val proToADCount: RDD[(String, (String, Int))] = proAndADCount.map(x => (x._1._1, (x._1._2, x._2)))

    // 7. 按照省份进行分组
    // 将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val proToADXCountList: RDD[(String, Iterable[(String, Int)])] = proToADCount.groupByKey()

    // 8.排序
    val result: RDD[(String, List[(String, Int)])] = proToADXCountList.mapValues(x => {
      x.toList.sortWith((a, b) => {
        a._2 > b._2
      }).take(3)
    }).sortByKey()

    result.collect().foreach(println)
  }
}
