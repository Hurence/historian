package com.hurence.historian.spark.sql.transformer


import org.apache.spark.sql.{Dataset}

trait Transformer[OPTIONS, INPUT,OUTPUT] {
def transform(options: OPTIONS , ds : Dataset[INPUT]): Dataset[OUTPUT] = { null }
}

