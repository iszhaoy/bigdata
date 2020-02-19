package com.iszhaoy.wc.com.iszhaoy.source

import com.iszhaoy.wc.iszhaoy.apitest.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class SensorSource extends  SourceFunction[SensorReading]{

  // flag：定义数据源是否还在正常运行
  var  running:Boolean = true

  // 正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 定义样例数据
    val rand = new Random()
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
    )

    while(running) {
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )

      // 获取当前时间戳
      val curTime: Long = System.currentTimeMillis()

      curTemp.foreach(
        // 发送
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(500)
    }

  }

  // 取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }
}
