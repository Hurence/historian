package com.hurence.timeseries.converter;

import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import com.hurence.timeseries.modele.points.PointImpl;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ChunkTruncater {

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    public static ChunkVersion0 truncate(ChunkVersion0 chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValueAsBinary();
        List<PointImpl> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);
        List<PointImpl> newPoints = points.stream()
                .filter(p -> {
                    return p.getTimestamp() > from && p.getTimestamp() < to;
                })
                .collect(Collectors.toList());
        PointsToChunkVersion0 converter = new PointsToChunkVersion0("ChunkTruncater");
        return converter.buildChunk(chunk.getName(), newPoints, chunk.getTags());
    }
}
