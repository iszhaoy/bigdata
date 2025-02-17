package com.iszhaoy.trans

import org.apache.spark.rdd.RDD

class Search(query:String) {
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    val query = this.query
    rdd.filter(x => x.contains(query))
  }

}
