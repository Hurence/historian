package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion1;
import com.hurence.timeseries.modele.Point;

import java.util.ArrayList;
import java.util.List;

public class GeneralVersion1SolrInjector extends AbstractVersion1SolrInjector {

    private List<ChunkModeleVersion1> chunks = new ArrayList<>();

    @Override
    protected List<ChunkModeleVersion1> buildListOfChunks() {
        return chunks;
    }

    public void addChunk(String metric, int year, int month, int day, String origin, List<Point> points) {
        ChunkModeleVersion1 chunk = ChunkModeleVersion1.fromPoints(metric, year, month, day, origin, points);
        chunks.add(chunk);
    }

    public void addChunk(String metric, String origin, List<Point> points) {
        ChunkModeleVersion1 chunk = ChunkModeleVersion1.fromPoints(metric, 1, 1, 1, origin, points);
        chunks.add(chunk);
    }

    public void addChunk(ChunkModeleVersion1 chunk) {
        chunks.add(chunk);
    }
}
