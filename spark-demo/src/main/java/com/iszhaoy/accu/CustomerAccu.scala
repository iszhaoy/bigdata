package com.iszhaoy.accu

import org.apache.spark.util.AccumulatorV2

class CustomerAccu extends AccumulatorV2[Int, Int] {

  // 定义一个属性
  var sum = 0

  // 判断是否为空
  override def isZero: Boolean = {
    this.sum == 0
  }

  // 复制一个累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu: CustomerAccu = new CustomerAccu
    accu.sum = this.sum
    accu
  }

  // 重置累加器
  override def reset(): Unit = {
    this.sum = 0
  }

  // 累加值
  override def add(v: Int): Unit = {
    this.sum += v
  }

  // 合并结果
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }

  // 获取累加值
  override def value: Int = {
    this.sum
  }
}
