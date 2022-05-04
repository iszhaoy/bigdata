package com.iszhaoy.joindim

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.Logger

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/8 15:26
 */

// 订单样例类
case class Order(time: Long, orderId: String, userId: String, goodsId: Int, price: Int, cityId: Int)

// 商品信息样例类 （可以定义一个标识位来判断是新增/更新还是删除）
case class Goods(goodsId: Int, goodsName: String, isRemove: Boolean)

object BroadCastConnectionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val orderStream: DataStream[Order] =
      env.socketTextStream("hadoop01", 9999)
      // 有状态算子一定要配置 uid
      .uid("order_topic_name")
      // 过滤为空的数据
      .filter(_ != null)
      // 解析数据
      .map(str => JSON.parseObject(str, classOf[Order]))

    val goodsDimStream: DataStream[Goods] =
      env.socketTextStream("hadoop01", 9998)
      // 有状态算子一定要配置 uid
      .uid("goods_dim_topic_name")
      .filter(_ != null)
      .map(str => JSON.parseObject(str, classOf[Goods]))

    orderStream.print("order")
    goodsDimStream.print("goods")

    /*
    * 定义存储 维度信息的 MapState，将订单流与商品映射信息的广播流进行connect，进行在process中进行关联。
    * process 中，广告流的处理逻辑是：将映射关系加入到状态中。事件流的处理逻辑是：
    * 从状态中获取当前商品 Id 对应的商品名称，拼接在一块发送到下游。最后打印输出。
    * */

    // 存储维度信息的MapState
    val GOODS_STATE: MapStateDescriptor[Integer, String] = new MapStateDescriptor[Integer, String](
      "GOODS_STATE",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)


    // 如果基于 Processing Time 做关联，我们可以利用 keyby 将两个数据流中关联字段值相同的数据划分到 KeyedCoProcessFunction 的同一个分区，然后用 ValueState 或者 MapState 将维表数据保存下来。
    // 在普通数据流的一条记录进到函数时，到 State 中查找有无符合条件的 join 对象，若有则关联输出结果，若无则根据 join 的类型决定是直接丢弃还是与空值关联。这里要注意的是，State 的大小要尽量控制好。
    // 首先是只保存每个 key 最新的维度数据值，其次是要给 State 设置好 TTL，让 Flink 可以自动清理。
    //orderStream.keyBy(_.goodsId).connect(goodsDimStream.keyBy(_.goodsId)).process(new KeyedCoProcessFunction[String,Order,Goods,(Order,String)] {
    //  override def processElement1(value: Order, ctx: KeyedCoProcessFunction[String, Order, Goods, (Order, String)]#Context, out: Collector[(Order, String)]): Unit = ???
    //
    //  override def processElement2(value: Goods, ctx: KeyedCoProcessFunction[String, Order, Goods, (Order, String)]#Context, out: Collector[(Order, String)]): Unit = ???
    //})

    val resStream: DataStream[(Order, String)] = orderStream
      // 订单流与 维度信息的广播流进行 connect
      .connect(goodsDimStream.broadcast(GOODS_STATE))
      .process(new BroadcastProcessFunction[Order, Goods, (Order, String)] {

        // 处理 订单信息，将订单信息与对应的商品名称进行拼接，一起发送到下游。
        override def processElement(order: Order, ctx: BroadcastProcessFunction[Order, Goods, (Order, String)
        ]#ReadOnlyContext, out: Collector[(Order, String)]) = {
          val broadcastState: ReadOnlyBroadcastState[Integer, String] = ctx.getBroadcastState(GOODS_STATE)
          // 从状态中获取 商品名称， 拼接后发送到下游
          val goodsName: String = broadcastState.get(order.goodsId)
          out.collect((order, goodsName))

        }

        // 更新商品的维表信息到状态中
        override def processBroadcastElement(goods: Goods, ctx: BroadcastProcessFunction[Order, Goods, (Order, String)
        ]#Context, out: Collector[(Order, String)]) = {
          val broadcastState: BroadcastState[Integer, String] = ctx.getBroadcastState(GOODS_STATE)
          // 商品上架，应该添加到状态中用户关联商品的信息
          // 商品下架，应该删除状态中用户关联商品的信息
          if (!goods.isRemove) broadcastState.put(goods.goodsId, goods.goodsName) else broadcastState.remove(goods.goodsId)
        }
      })

    // 结果进行打印，生产环境应该是输出到外部存储
    resStream.print("res")

    /*
    * ps：
    * 当维度信息较大，每台机器上都存储全量维度信息导致内存压力过大时，可以考虑进行 keyBy，这样每台节点只会存储当前 key 对应的维度信息，
    * 但是使用 keyBy 会导致所有数据都会进行 shuffle。当然上述代码需要将维度数据广播到所有实例，也是一种 shuffle，但是维度变更一般只是少量数据，
    * 成本较低，可以接受。大家在开发 Flink 任务时应该根据实际的业务场景选择最合适的方案。
    * */
    env.execute("job")
  }
}