import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/5/18 16:54
 * @version 1.0
 */
object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单时间流
    val orderEvent1Stream: KeyedStream[OrderEvent1, String] = env.readTextFile(getClass.getResource("/OrderLog.csv").getPath)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent1(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(getClass.getResource("/ReceiptLog.csv").getPath)
      .map((data: String) => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream: DataStream[(OrderEvent1, ReceiptEvent)] = orderEvent1Stream.intervalJoin(receiptStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()

    env.execute("TxMatchByJoin")

  }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent1, ReceiptEvent, (OrderEvent1, ReceiptEvent)] {
  override def processElement(left: OrderEvent1, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent1, ReceiptEvent,
    (OrderEvent1, ReceiptEvent)]#Context, out: Collector[(OrderEvent1, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}