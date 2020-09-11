package com.hurence.timeseries.converter;

import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import com.hurence.timeseries.modele.chunk.ChunkVersionEVOA0;
import com.hurence.timeseries.modele.points.PointImpl;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ChunkTruncater {

    public final static String TRUNCATER_ORIGIN = "ChunkTruncater";
    /**
     * The chunk in input must have getValueAsBinary implemented (not returning null)
     * The chunk in input must have getName implemented (not returning null)
     * The chunk in input must have getTags implemented (not returning null)
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    public static ChunkVersion0 truncate(ChunkVersion0 chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValueAsBinary();
        TreeSet<PointImpl> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);///
        PointsToChunkVersion0 converter = new PointsToChunkVersion0(TRUNCATER_ORIGIN);
        return converter.buildChunk(chunk.getName(), points, chunk.getTags());
    }

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    public static ChunkVersionEVOA0 truncate(ChunkVersionEVOA0 chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValueAsBinary();
        TreeSet<PointImpl> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);
        PointsToChunkVersionEVOA0 converter = new PointsToChunkVersionEVOA0();
        return converter.buildChunk(chunk.getName(), points, chunk.getTags());
    }
}
