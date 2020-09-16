package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersionEVOA0;
import com.hurence.timeseries.model.Measure;

import java.util.ArrayList;
import java.util.List;

public class GeneralEVOA0SolrInjector extends AbstractEVOA0SolrInjector {

    private List<ChunkModeleVersionEVOA0> chunks = new ArrayList<>();

    @Override
    protected List<ChunkModeleVersionEVOA0> buildListOfChunks() {
        return chunks;
    }


    public void addChunk(String metric, int year, int month, int day, String origin, List<Measure> measures) {
        ChunkModeleVersionEVOA0 chunk = ChunkModeleVersionEVOA0.fromPoints(metric, year, month, day, origin, measures);
        chunks.add(chunk);
    }

    public void addChunk(String metric, String origin, List<Measure> measures) {
        ChunkModeleVersionEVOA0 chunk = ChunkModeleVersionEVOA0.fromPoints(metric, 1, 1, 1, origin, measures);
        chunks.add(chunk);
    }

    public void addChunk(ChunkModeleVersionEVOA0 chunk) {
        chunks.add(chunk);
    }
}
