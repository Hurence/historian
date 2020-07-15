package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.modele.PointImpl;

import java.util.ArrayList;
import java.util.List;

public class GeneralVersion0SolrInjector extends AbstractVersion0SolrInjector {

    private List<ChunkModeleVersion0> chunks = new ArrayList<>();

    @Override
    protected List<ChunkModeleVersion0> buildListOfChunks() {
        return chunks;
    }

    public void addChunk(String metric, int year, int month, int day, String origin, List<Point> points) {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints(metric, year, month, day, origin, points);
        chunks.add(chunk);
    }

    public void addChunk(String metric, String origin, List<Point> points) {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints(metric, 1, 1, 1, origin, points);
        chunks.add(chunk);
    }

    public void addChunk(ChunkModeleVersion0 chunk) {
        chunks.add(chunk);
    }
}
