package com.hurence.historian


import org.apache.spark.Partitioner

class MetricPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }


  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: MetricPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}