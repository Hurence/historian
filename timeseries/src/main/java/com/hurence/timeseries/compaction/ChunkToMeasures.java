package com.hurence.timeseries.compaction;

import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.IOException;
import java.io.InputStream;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkToMeasures /* implements Chunker<Measure, Chunk> */ {


    private Chunk.ChunkBuilder computeStats(TreeSet<Measure> measures, Chunk.ChunkBuilder builder) {

        SummaryStatistics valueStats = new SummaryStatistics();
        SummaryStatistics qualityStats = new SummaryStatistics();
        AtomicBoolean hasQuality = new AtomicBoolean(false);

        measures.forEach(m -> {
            valueStats.addValue(m.getValue());

            if (m.hasQuality()) {
                hasQuality.set(true);
                qualityStats.addValue(m.getQuality());
            }
        });

        builder.avg(valueStats.getMean())
                .std(valueStats.getStandardDeviation())
                .sum(valueStats.getSum())
                .min(valueStats.getMin())
                .max(valueStats.getMax())
                .first(measures.first().getValue())
                .last(measures.last().getValue());

        if (hasQuality.get()) {
            builder.qualityMin((float) qualityStats.getMin())
                    .qualityMax((float) qualityStats.getMax())
                    .qualityAvg((float) qualityStats.getMean())
                    .qualityFirst(measures.first().getQuality())
                    .qualitySum((float) qualityStats.getSum());
        }

        return builder;
    }


    //  @Override
    public Chunk chunk(TreeSet<Measure> measures) {

        assert !measures.isEmpty() : "Measure's list cannot be empty if you want to build a Chunk";


        byte[] serializedData = ProtoBufTimeSeriesSerializer.to(measures.iterator());
        byte[] compressedData = Compression.compress(serializedData);

        Chunk.ChunkBuilder builder = Chunk.builder()
                .name(measures.first().getName())
                .tags(measures.first().getTags())
                .day(measures.first().getDay())
                .valueBinaries(compressedData);

        computeStats(measures, builder);

        return builder
                .computeMetrics()
                .buildId()
                .build();
    }

    // @Override
    public TreeSet<Measure> unchunk(Chunk chunk) throws IOException {

        try (InputStream decompressed = Compression.decompressToStream(chunk.getValueBinaries())) {
            return ProtoBufTimeSeriesSerializer.from(decompressed, chunk.getStart(), chunk.getEnd());
        }


    }
}
