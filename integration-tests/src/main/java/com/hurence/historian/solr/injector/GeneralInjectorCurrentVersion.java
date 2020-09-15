package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class GeneralInjectorCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private List<ChunkVersionCurrent> chunks = new ArrayList<>();
    @Override
    protected List<ChunkVersionCurrent> buildListOfChunks() {
        return chunks;
    }

    public void addChunk(String metric, String origin, List<Point> points) {
        final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent(origin);
        ChunkVersionCurrent chunk = converter.buildChunk(metric, new TreeSet(points));
        chunks.add(chunk);
    }

    public void addChunk(ChunkVersionCurrent chunk) {
        chunks.add(chunk);
    }

}
