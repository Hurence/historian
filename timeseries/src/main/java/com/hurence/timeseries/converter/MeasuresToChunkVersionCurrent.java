package com.hurence.timeseries.converter;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.DateInfo;
import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.TimeSeriesUtil;
import com.hurence.timeseries.analysis.TimeseriesAnalysis;
import com.hurence.timeseries.analysis.TimeseriesAnalyzer;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.sax.SaxConverter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public class MeasuresToChunkVersionCurrent implements MeasuresToChunk {

    private String chunkOrigin;

    public MeasuresToChunkVersionCurrent(String chunkOrigin) {
        this.chunkOrigin = chunkOrigin;
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }

    /**
     * @param name
     * @param measures expected to be f the same year, month, day
     * @param tags
     * @return
     */
    public Chunk buildChunk(String name, TreeSet<? extends Measure> measures, Map<String, String> tags) {
        if (measures == null || measures.isEmpty())
            throw new IllegalArgumentException("points should not be null or empty");
        MetricTimeSeries chunk = buildMetricTimeSeries(name, measures);
        return convertIntoChunk(chunk, tags);
    }

    @Override
    public Chunk buildChunk(String name,
                            TreeSet<? extends Measure> measures) {
        return buildChunk(name, measures, Collections.emptyMap());
    }


    private Chunk convertIntoChunk(MetricTimeSeries timeSerie, Map<String, String> tags) {


        Chunk.ChunkBuilder builder = Chunk.builder();
        byte[] compressedPoints = BinaryCompactionUtil.serializeTimeseries(timeSerie);
        builder
                .origin(this.chunkOrigin)
                .tags(tags)
                .end(timeSerie.getEnd())
                .name(timeSerie.getName())
                .start(timeSerie.getStart())
                .value(compressedPoints)
                .version(getVersion());

        computeAndSetAggs(builder, timeSerie);
        DateInfo dateInfo = TimeSeriesUtil.calculDateFields(timeSerie.getStart());
        builder
                .year(dateInfo.year)
                .month(dateInfo.month)
                .day(dateInfo.day)
                .buildId();

        return builder.build();
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    private void computeAndSetAggs(Chunk.ChunkBuilder builder, MetricTimeSeries timeSeries) {
        Integer sax_alphabet_size = Math.max(Math.min(timeSeries.size(), 7), 2);
        Integer sax_string_length = Math.min(timeSeries.size(), 100);

        timeSeries.sort();
        final List<Double> values = timeSeries.getValues().toList();
        final List<Double> qualities = timeSeries.getQualities().toList();
        final List<Long> timestamps = timeSeries.getTimestamps().toList();


        // set quality stats if needed
        TimeseriesAnalysis qualitiesAnalysis = TimeseriesAnalyzer.builder()
                .computeOutlier(false)
                .computeStats(true)
                .computeTrend(false)
                .build()
                .run(timestamps, qualities);

        if (qualitiesAnalysis.getCount() > 0) {
            builder.qualityFirst(timeSeries.getQuality(0));
            builder.qualitySum((float) qualitiesAnalysis.getSum());
            builder.qualityMin((float) qualitiesAnalysis.getMin());
            builder.qualityMax((float) qualitiesAnalysis.getMax());
            builder.qualityAvg((float) qualitiesAnalysis.getMean());
        }

        // compute stats for values
        TimeseriesAnalysis valuesAnalysis = TimeseriesAnalyzer.builder()
                .computeOutlier(true)
                .computeStats(true)
                .computeTrend(true)
                .build()
                .run(timestamps, values);


        builder.first(valuesAnalysis.getFirst());
        builder.last(valuesAnalysis.getLast());
        builder.min(valuesAnalysis.getMin());
        builder.max(valuesAnalysis.getMax());
        builder.sum(valuesAnalysis.getSum());
        builder.avg(valuesAnalysis.getMean());
        builder.count(valuesAnalysis.getCount());
        builder.stdDev(valuesAnalysis.getStdDev());
        builder.trend(valuesAnalysis.isHasTrend());
        builder.outlier(valuesAnalysis.isHasOutlier());


        // finish with sax
        SaxConverter converter = SaxConverter.builder()
                .alphabetSize(sax_alphabet_size)
                .paaSize(sax_string_length)
                .build();

        String sax = converter.run(values);
        builder.sax(sax);
    }


    private MetricTimeSeries buildMetricTimeSeries(String name, TreeSet<? extends Measure> points) {
        final long start = getStart(points);
        final long end = getEnd(points);
        MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(name);
        tsBuilder.start(start);
        tsBuilder.end(end);
        points.forEach(p -> {
            if (p.hasQuality())
                tsBuilder.point(p.getTimestamp(), p.getValue(), p.getQuality());
            else
                tsBuilder.point(p.getTimestamp(), p.getValue());
        });
        return tsBuilder.build();
    }

    private long getEnd(TreeSet<? extends Measure> points) {
        return points.last().getTimestamp();
    }

    private long getStart(TreeSet<? extends Measure> points) {
        return points.first().getTimestamp();
    }
}
