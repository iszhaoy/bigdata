package com.iszhaoy.mysql

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMysqlRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 3.定义连接mysql参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://bigdata01:3306/rdd"
    val userName = "root"
    val passWd = "root"

    // 4.创建Mysql的RDD
    val jdbcRDD = new JdbcRDD[String](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from rdd where name = ? and name = ?",
      0,
      0,
      1,
      (x: ResultSet) =>
        x.getString(1)
    )
    jdbcRDD.foreach(println)

    sc.stop()
  }
}
