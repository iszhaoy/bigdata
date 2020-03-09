package com.test.orderpay_detect

import com.test.orderpay_detect.OUtTimeoutWithoutCep.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/3/13 17:19
 * @version 1.0
 */

// 定义接受流事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单时间流
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      //    val orderEventStream = env.socketTextStream("hadoop01", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResuorce = getClass.getResource("/ReceiptLog.csv")


  }
}
