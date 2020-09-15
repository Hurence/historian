package com.hurence.timeseries.converter;

import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionEVOA0;
import com.hurence.timeseries.modele.points.Point;

import java.io.IOException;
import java.util.TreeSet;

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
    public static ChunkVersionCurrent truncate(ChunkVersionCurrent chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValueAsBinary();
        TreeSet<Point> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);///
        PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent(TRUNCATER_ORIGIN);
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
        TreeSet<Point> points = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);
        PointsToChunkVersionEVOA0 converter = new PointsToChunkVersionEVOA0();
        return converter.buildChunk(chunk.getName(), points, chunk.getTags());
    }
}
