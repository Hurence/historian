package com.hurence.timeseries.converter;

import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;

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
    public static Chunk truncate(Chunk chunk, long from, long to) throws IOException {
        byte[] binaries = chunk.getValue();
        TreeSet<Measure> measures = BinaryCompactionUtil.unCompressPoints(binaries, chunk.getStart(), chunk.getEnd(), from, to);///
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent(TRUNCATER_ORIGIN);
        return converter.buildChunk(chunk.getName(), measures, chunk.getTags());
    }


}
