package com.iszhaoy.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object CustomerAvg  extends  UserDefinedAggregateFunction{

  // 输入数据的类型
  override def inputSchema: StructType = StructType(StructField("input",LongType)::Nil)

  // 缓存数据的类型
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)
    ::StructField("count",LongType)::Nil)

  // 输出数据的类型
  override def dataType: DataType = DoubleType

  // 函数确定性
  override def deterministic: Boolean = true

  // 初始化缓存
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 分区内累加值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    // 判断输入是否为空
    if(!input.isNullAt(0)) {
      buffer(0)  = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 分区间累加值
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
