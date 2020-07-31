package com.hurence.timeseries.converter;

import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.points.PointImpl;

import java.io.IOException;
import java.util.List;

public class ChunkTruncater {

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    public static Chunk truncate(Chunk chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValueAsBinary();
        List<PointImpl> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);
        //TODO calculate aggs
        return null;//TODO
    }

//    public static Chunk buildChunkFromPoints(List<Point> points) {
//        MetricTimeSeries chunk = buildMetricTimeSeries(json);
//        return convertIntoSolrInputDocument(chunk);
//        return null;//TODO
//    }
}
