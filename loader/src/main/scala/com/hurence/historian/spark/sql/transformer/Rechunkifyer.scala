package com.hurence.historian.spark.sql.transformer

import com.hurence.historian.modele.ChunkRecordV0
import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}
import org.apache.spark.sql._



class Rechunkifyer extends Transformer[RechunkifyerOptions, ChunkRecordV0, ChunkRecordV0] {

  /**
   *Chunkifyer used to transform the size of chunks
   *
   *
   * @param options
   */
 override def transform (options : RechunkifyerOptions, data : Dataset[ChunkRecordV0]): Dataset[ChunkRecordV0]={

   val CHUNK_NEW_SIZE =  options.OUTPUT_CHUNK_SIZE

   val spark = SparkSession.builder
      .getOrCreate()

   import spark.implicits._

   val unchunkyfier = new UnChunkyfier()
     .setChunkCol("chunk")

   val unchunkifyedToRechunkify = unchunkyfier.transform(data)

   val rechunkify = new Chunkyfier()
     .setGroupByCols(Array("name", "tags.metric_id"))
    .setChunkMaxSize(CHUNK_NEW_SIZE)

   val newBucketedData = rechunkify.transform(unchunkifyedToRechunkify).as[ChunkRecordV0]
   newBucketedData

}
}
