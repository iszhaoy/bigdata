package com.iszhaoy.partitioner

import org.apache.spark.Partitioner

class CustomrPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  // spark不
  override def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => key.hashCode() % partitions - 1;
    }
  }
}
