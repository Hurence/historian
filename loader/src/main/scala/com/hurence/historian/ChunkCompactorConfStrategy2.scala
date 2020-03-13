package com.hurence.historian

case class ChunkCompactorConfStrategy2(zkHosts: String,
                                       collectionName: String,
                                       chunkSize: Int,
                                       saxAlphabetSize: Int,
                                       saxStringLength: Int,
                                       year: Int,
                                       month: Int,
                                       day: Int,
                                       solrFq: String)
