package com.iszhaoy.exec

import org.apache.spark.{SparkConf, SparkContext}

object TestMapAndMapPartition {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Seq(("a", 1), ("b", 1), ("a", 1)))

    var flag = 0

    // 无法操作map外的数据
    rdd.map(data => {
      flag += 1
      println(data)
    }).collect()
    println(flag)

    rdd.mapPartitions(p => {
      var flag = 0
      val deal = p.map {
        flag += 1
        println(_)
      }
      println(s"""partition $flag""")
      deal
    }).collect()
    println(flag)

    sc.stop()
  }
}
