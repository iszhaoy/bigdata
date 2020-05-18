import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/5/18 14:16
 * @version 1.0
 */

case class OrderEvent1(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatch {
  val unmatchPays: OutputTag[OrderEvent1] = new OutputTag[OrderEvent1]("unmatchPays")
  val unmatchReceipt: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatchReceipt")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEvent1Stream = env.readTextFile(getClass.getResource("OrderLog.csv").getPath)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent1(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptLogStream = env
      .readTextFile(getClass.getResource("/ReceiptLog.csv").getPath)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    val processStream = orderEvent1Stream
      .connect(receiptLogStream)
      .process(new CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1, ReceiptEvent)] {

        lazy val payedState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent1]("payedState",
          classOf[OrderEvent1]))
        lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptState",
          classOf[ReceiptEvent]))

        override def processElement1(value: OrderEvent1, ctx: CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1,
          ReceiptEvent)]#Context, out: Collector[(OrderEvent1, ReceiptEvent)]) = {

          val receipt = receiptState.value()

          if (receipt != null) {
            out.collect((value, receipt))
          } else {
            payedState.update(value)
            ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
          }
        }

        override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent1, ReceiptEvent,
          (OrderEvent1, ReceiptEvent)]#Context, out: Collector[(OrderEvent1, ReceiptEvent)]) = {
          val pays = payedState.value()
          if (pays != null) {
            out.collect((pays, value))
          } else {
            receiptState.update(value)
            ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
          }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent1, ReceiptEvent, (OrderEvent1,
          ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent1, ReceiptEvent)]) = {
          if (payedState.value() != null) {
            // receipt没有来
            ctx.output(unmatchPays, payedState.value())
          }
          if (receiptState.value() != null) {
            // receipt没有来
            ctx.output(unmatchReceipt, receiptState.value())
          }
        }
      })

    processStream.print("match")
    processStream.getSideOutput(unmatchPays).print("unmatchPays")
    processStream.getSideOutput(unmatchReceipt).print("unmatchReceipt")

    env.execute("TxMatch")
  }


}
