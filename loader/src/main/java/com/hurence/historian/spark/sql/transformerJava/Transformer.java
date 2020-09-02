package com.hurence.historian.spark.sql.transformerJava;


import org.apache.spark.sql.Dataset;

public interface Transformer<OPTION,INPUT,OUTPUT>{

     Dataset<OUTPUT> transform(OPTION opt, Dataset<INPUT> ds);
}
