package com.iszhaoy.exec

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:
 * 数据说明：
 *   users.dat ---UserID::Gender::Age::Occupation::Zip-code
 *   movies.dat --- MovieID::Title::Genres
 *   ratings.dat ---UserID::MovieID::Rating::Timestamp
 * 需求：
 * 1：评分（平均分）最高的10部电影
 * 2：18 - 24 岁的男性年轻人 最喜欢看的10部电影
 * 3：女性观看最多的10部电影名称及观看次数
 */
object MovieLogAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MovieLogAnalyzer").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val userFiles: RDD[String] = sc.textFile(args(0))
    val moviesFiles: RDD[String] = sc.textFile(args(1))
    val ratingsFiles: RDD[String] = sc.textFile(args(2))

    //整理用户数据,对用户数据进行切分
    val userRdd: RDD[(Int, String, Int)] = getUserRDD(userFiles)
    //整理电影数据,对用户数据进行切分
    val movieRdd: RDD[(Int, String)] = getMovieRDD(moviesFiles)
    //对评分数据进行整理
    val ratingRdd: RDD[(Int, Int, Double)] = getRatingsRDD(ratingsFiles)

    println("-----------------1：评分（平均分）最高的10部电影---------------------------")
    //所有的电影及评分数据
    val movieAndRating: RDD[(Int, Double)] = getMovieAndRateTotal(ratingRdd)
    val movieAndRatingResult: Array[(Int, Double)] = movieAndRating.sortBy(-_._2).take(10)
    println(s"评分（平均分）最高的10部电影：${movieAndRatingResult.mkString(",")}")

    println("-----------------2：18 - 24 岁的男性年轻人 最喜欢看的10部电影----------------")
    //满足条件的所有电影和观看次数（评分次数）
    val freshMaleMovie: RDD[(Int, Int)] = getFreshMaleMovie(sc, ratingRdd, userRdd)
    val freshMaleMovieResult: Array[(Int, Int)] = freshMaleMovie.sortBy(-_._2).take(10)
    println(s"18 - 24 岁的男性年轻人 最喜欢看的10部电影：${freshMaleMovieResult.mkString(",")}")


    println("-----------------3：女性观看最多的10部电影名称及观看次数----------------")
    val famaleMovieTitle: RDD[(String, Int)] = getFamaleMovieTitle(sc, ratingRdd, userRdd, movieRdd)
    val famaleMovieTitleResult: Array[(String, Int)] = famaleMovieTitle.sortBy(-_._2).take(10)
    println(s"女性观看最多的10部电影名称及观看次数：${famaleMovieTitleResult.mkString(",")}")

    sc.stop()
  }

  /**
   * 女性观看最多的10部电影名称及观看次数
   *
   * @param sc
   * @param ratingRdd
   * @param userRdd
   * @param movieRdd
   */
  def getFamaleMovieTitle(sc: SparkContext, ratingRdd: RDD[(Int, Int, Double)], userRdd: RDD[(Int, String, Int)], movieRdd: RDD[(Int, String)]) = {
    //过滤女性用户
    val famaleRDD: RDD[(Int, String, Int)] = userRdd.filter(t => ("F".equals(t._2)))
    //提取出来uid,利用set或者distince去重
    val famaleSet: Set[Int] = famaleRDD.map(_._1).collect().toSet
    val famaleRef: Broadcast[Set[Int]] = sc.broadcast(famaleSet)

    //过滤出俩所有女性看过的电影
    val fUserMovie: RDD[(Int, Int, Double)] = ratingRdd.filter(t => famaleRef.value.contains(t._1))

    //聚合统计,返回女性观看的电影及观看次数汇总
    val fMovieOrder: RDD[(Int, Int)] = fUserMovie.map(t => (t._2, 1)).reduceByKey(_ + _)
    //(电影id,(观看次数,电影名称))
    val joinRDD: RDD[(Int, (Int, String))] = fMovieOrder.join(movieRdd)

    //返回(电影名称,观看次数)
    joinRDD.map(t => (t._2._2, t._2._1))

  }

  /**
   * 18 - 24 岁的男性年轻人 最喜欢看的10部电影
   *
   * @param sc
   * @param ratingRdd
   * @param userRdd
   */
  def getFreshMaleMovie(sc: SparkContext, ratingRdd: RDD[(Int, Int, Double)], userRdd: RDD[(Int, String, Int)]) = {
    //过滤18-24岁的男青年
    val freshMan: RDD[(Int, String, Int)] = userRdd.filter(t => "M".equals(t._2) && t._3 >= 18 && t._3 <= 24)

    //提取出来uid,利用set或者distince去重
    val freshSet: Set[Int] = freshMan.map(_._1).collect().toSet
    val freshRef: Broadcast[Set[Int]] = sc.broadcast(freshSet)

    //从评分数据中,提取电影id,用户id,并判断出来满足条件的userid
    val movieAndOne: RDD[(Int, Int)] = ratingRdd.map(t => (t._1, t._2)).filter({
      case (uid, _) => {
        val freshInExecutor = freshRef.value
        //判断uid是否是18--24的男青年
        freshInExecutor.contains(uid)
      }
    }).map(t => (t._2, 1))
    //分组聚合
    movieAndOne.reduceByKey(_ + _)
  }

  /**
   * 1：评分（平均分）最高的10部电影
   *
   * @param ratingRdd
   */
  def getMovieAndRateTotal(ratingRdd: RDD[(Int, Int, Double)]) = {
    //数据预处理,获取(电影id,(评分,1)),方便使用reduceBykey
    val movieAndRating: RDD[(Int, (Double, Int))] = ratingRdd.map(t => (t._2, (t._3, 1)))

    //分组聚合(电影id,(电影的总评分,评分次数))
    val reduceRdd = movieAndRating.reduceByKey((r1, r2) => {
      //获取总的评分
      val sunRating = r1._1 + r2._1
      //评分次数
      val count = r1._2 + r2._2
      (sunRating, count)
    })
    //获取评分平均值
    reduceRdd.map { case (movie, (sum, len)) => (movie, sum.toDouble / len) }
  }

  /**
   * 在用户志中获取用户基本资料信息
   *
   * @param userFiles
   */
  def getUserRDD(userFiles: RDD[String]) = {
    userFiles.map(line => {
      val fields: Array[String] = line.split("::")
      val uid = fields(0).toInt
      val gender = fields(1)
      val age = fields(2).toInt
      //取出其中扥用户id,性别,年龄
      (uid, gender, age)
    }).cache()
  }

  /**
   * 在电影日志中获取电影基本信息
   *
   * @param moviesFiles
   */
  def getMovieRDD(moviesFiles: RDD[String]) = {
    moviesFiles.map(line => {
      val fields = line.split("::")
      val mid = fields(0).toInt
      val title = fields(1)
      //获取电影信息
      (mid, title)
    }).cache()
  }

  /**
   * 在评分日志中获取用户id,电影id,评分id关联数据
   *
   * @param ratingsFiles
   */
  def getRatingsRDD(ratingsFiles: RDD[String]) = {
    ratingsFiles.map(line => {
      val fields = line.split("::")
      val uid = fields(0).toInt
      val mid = fields(1).toInt
      val rating = fields(2).toDouble
      //获取用户id,电影id,评分id关联数据
      (uid, mid, rating)
    }).cache()
  }
}
