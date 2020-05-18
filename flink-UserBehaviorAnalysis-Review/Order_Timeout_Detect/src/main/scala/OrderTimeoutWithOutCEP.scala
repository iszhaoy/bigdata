import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/5/18 11:40
 * @version 1.0
 */
object OrderTimeoutWithOutCEP {
  val orderTimeoutOutPutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeoutTag")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val orderEventStream = env.readTextFile(getClass.getResource("OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    val resultDataStream: DataStream[OrderResult] = orderEventStream.process(new OrderTimeoutWarning())
    resultDataStream
      .print("success")
    resultDataStream.getSideOutput(orderTimeoutOutPutTag).print("timeout")

    env.execute("OrderTimeoutWithOutCEP")
  }


  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    // 保存pay是否来过的状态
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]
    ("ispayedState", classOf[Boolean]))

    // 保存定时器的时间戳为状态，是否来过的状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState",
      classOf[Long]))


    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()
      val timer = timerState.value()
      // 根据事件的类型进行分类判断，做不同的逻辑处理
      if (value.eventType == "create") {
        // 如果是create时间，接下来判断pay是否来过
        if (isPayed) {
          // 1.1 如果已经匹配成功，输出主流数据，清空状态
          out.collect(OrderResult(value.orderId, "payed successful"))
          ctx.timerService().deleteEventTimeTimer(timer)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 1.2 如果没有pay过，注册定时器，等待定时器的到来
          val ts = value.eventTime * 1000L + 15 * 60 * 1000
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 2 如果是pay事件，判断是否有create过，用timer判断
        if (timer > 0) {
          // 2.1 如果有定时器，说明已经有create来过
          // 继续判断，是否超过timeout时间
          if (timer > value.eventTime * 1000L) {
            // 2.1.1 如果定时器时间还没到，那么输出成功匹配
            out.collect(OrderResult(value.orderId, "payed successful"))
          } else {
            // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutPutTag, OrderResult(value.orderId, "order timeout"))
          }
          // 输出结束清空状态
          isPayedState.clear()
          timerState.clear()
        } else {
          // 2.2 pay先到了,更新状态,注册定时器等待create
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L) // 不懂
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没来
      if (isPayedState.value()) {
        // 如果为true,表示pay先到了，没等到create
        ctx.output(orderTimeoutOutPutTag, OrderResult(ctx.getCurrentKey, "alrealy payed but not found create"))
      } else {
        ctx.output(orderTimeoutOutPutTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }

}

class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 定义状态
  // 保存pay是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]
  ("ispayedState", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              out: Collector[OrderResult]): Unit = {
    // 取出状态
    val isPayed: Boolean = isPayedState.value()

    if (value.eventType == "create" && !isPayed) {
      // 如果遇到了create时间，并且pay没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      // 如果为pay时间，直接把状态改成true
      isPayedState.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
    OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "successful!!!"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "time out"))
    }
    isPayedState.clear()
  }
}