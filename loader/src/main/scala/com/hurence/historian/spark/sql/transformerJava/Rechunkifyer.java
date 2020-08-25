package com.hurence.historian.spark.sql.transformerJava;

import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import org.apache.spark.sql.Dataset;

public class Rechunkifyer implements Transformer<RechunkOptions, ChunkVersion0, ChunkVersion0> {

    @Override
    public Dataset<ChunkVersion0> transform(RechunkOptions opt, Dataset<ChunkVersion0> ds) {
        return null;
    }
}
