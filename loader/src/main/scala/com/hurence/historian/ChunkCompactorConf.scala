package com.hurence.historian

case class ChunkCompactorConf(zkHosts: String,
                              collectionName: String,
                              chunkSize: Int,
                              saxAlphabetSize: Int,
                              saxStringLength: Int,
                              year: Int,
                              month: Int,
                              day: Int)