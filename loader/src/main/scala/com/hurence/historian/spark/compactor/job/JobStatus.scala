package com.hurence.historian.spark.compactor.job

object JobStatus extends Enumeration {
  val RUNNING, SUCCEEDED, FAILED = Value
}
