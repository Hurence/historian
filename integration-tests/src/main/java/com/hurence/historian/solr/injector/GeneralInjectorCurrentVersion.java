package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class GeneralInjectorCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private List<Chunk> chunks = new ArrayList<>();
    @Override
    protected List<Chunk> buildListOfChunks() {
        return chunks;
    }

    public void addChunk(String metric, String origin, List<Chunk> points) {
        final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent(origin);
        Chunk chunk = converter.buildChunk(metric, new TreeSet(points));
        chunks.add(chunk);
    }

    public void addChunk(Chunk chunk) {
        chunks.add(chunk);
    }

}
