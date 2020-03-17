package com.test.orderpay_detect

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

  // 定义侧输出流的tag
  // 有订单事件，没有到账事件
  val unmatchPays = new OutputTag[OrderEvent]("unmatchPays")
  // 没有订单事件，有到账事件
  val unmatchReceipt = new OutputTag[ReceiptEvent]("unmatchReceipt")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单时间流
    val resource: URL = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
    val orderEventStream: KeyedStream[OrderEvent, String] = env.socketTextStream("localhost", 7777)
      //    val orderEventStream = env.socketTextStream("hadoop01", 7777)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResuorce: URL = getClass.getResource("/ReceiptLog.csv")
    //    val receiptStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResuorce.getPath)
    val receiptStream: KeyedStream[ReceiptEvent, String] = env.socketTextStream("localhost", 8888)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两流连接起来
    val processedStream = orderEventStream.connect(receiptStream)
      .process(new TxPayMatch())

    processedStream.print("match")

    processedStream.getSideOutput(unmatchReceipt).print("unmatchReceipt")
    processedStream.getSideOutput(unmatchPays).print("unmatchPays")

    env.execute("processedStream job")

  }


  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 定义状态控制两条流里面的数据
    // 定义状态来保存已经到达的订单支付事件和到账事件

    lazy val payState: ValueState[OrderEvent] = getRuntimeContext
      .getState(new ValueStateDescriptor[OrderEvent]("payState", classOf[OrderEvent]))

    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext
      .getState(new ValueStateDescriptor[ReceiptEvent]("receiptState", classOf[ReceiptEvent]))

    // 订单事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent,
      ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      // 判断有没有对应的到账事件
      val receipt = receiptState.value()

      if (receipt != null) {
        // 如果已经有receipt,在主流输出匹配信息,清空状态
        out.collect((pay, receipt))
        receiptState.value()
      } else {
        // 如果receipt还没有到,那么把pay存入状态，并且注册一个定时器等待
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    // 到账事件的处理
    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent,
      ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay = payState.value()

      if (pay != null) {
        out.collect((pay, value))
        payState.clear()
      } else {
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)
    ]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 定时器触发，如果还没有收到某个事件。那么输出报警
      if (payState.value() != null) {
        // receipt没有来
        ctx.output(unmatchPays, payState.value())
      }

      if (receiptState.value() != null) {
        // receipt没有来
        ctx.output(unmatchReceipt, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }

  }

}

