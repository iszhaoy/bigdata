package com.iszhaoy.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestPartitioner {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("Agent").setMaster("local[*]")

    // 2.创建SparkConf
    val sc = new SparkContext(conf)

    //3.创建RDD
    val value: RDD[(Int, Int)] = sc.parallelize(Array((1,2),(2,3),(3,4),(4,5)))

    // 4. 打印value的分区情况
    value.mapPartitionsWithIndex((index,items)=>{
      items.map((index,_))
    }).foreach(println)

    // 对value进行重新分区
    val result: RDD[(Int, Int)] = value.partitionBy(new CustomrPartitioner(5))
    result.mapPartitionsWithIndex((index,items)=>{
        items.map((index,_))
      }).foreach(println)

    sc.stop()
  }
}
