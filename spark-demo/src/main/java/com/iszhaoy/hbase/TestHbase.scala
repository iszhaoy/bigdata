package com.iszhaoy.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TestHbase {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    // 3.连接hbase
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03")
    conf.set(TableInputFormat.INPUT_TABLE, "stu")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc
      .newAPIHadoopRDD[ImmutableBytesWritable, Result, TableInputFormat](
        conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // 5.打印数据
        hbaseRDD.foreach{
          case (_,result)=> {
            println(Bytes.toString(result.getRow))
          }
        }

    // hadoop的序列化机制和spark不一样 会报错
    //    val value: RDD[(String, Result)] = hbaseRDD.map(x => {
    //      ("a", x._2)
    //    })

    //    value.partitionBy(new HashPartitioner(2)).foreach(x => println(x._2.getRow))

    sc.stop()
  }
}
