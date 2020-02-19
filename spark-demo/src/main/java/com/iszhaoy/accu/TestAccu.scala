package com.iszhaoy.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 3.定义一个变量
    var sum: LongAccumulator = sc.longAccumulator("sum")

    //4. 创建一个RDD
    val numRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4),2)

    // 5. 转换为元组并将变量自增
    val numToOne: RDD[(Int, Int)] = numRDD.map(x => {
      sum.add(1L)
      (x, 1)
    })

    // 6. 打印结果
    numToOne.foreach(println)

    // 7. 打印变量
    println(sum.value)
    // 8. 关闭链接
    sc.stop()

  }
}
