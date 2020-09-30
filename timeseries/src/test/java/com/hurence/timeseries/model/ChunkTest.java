package com.hurence.timeseries.model;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.converter.ChunkToMeasures;
import com.hurence.timeseries.converter.ChunkToMeasuresConverter;
import com.hurence.timeseries.converter.MeasuresToChunk;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.*;

public class ChunkTest {

    private static final Logger logger = LoggerFactory.getLogger(ChunkTest.class);

    @Test
    public void testTruncate() {
        Map<String, String> tags = new HashMap<String, String>() {{
            put("couNtry", "France");
            put("usine", "usine 2 ;;Alpha go");
        }};
        MeasuresToChunk converter = new MeasuresToChunkVersionCurrent("test");
        Chunk chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(1, 1),
                        Measure.fromValue(10, 2),
                        Measure.fromValue(100, 3),
                        Measure.fromValue(200, 4),
                        Measure.fromValue(300, 5),
                        Measure.fromValue(500, 6),
                        Measure.fromValue(600, 7),
                        Measure.fromValue(800, 8),
                        Measure.fromValue(1000, 9),
                        Measure.fromValue(1111, 10)
                )),
                tags
        );
        //        origin chunk
        Assertions.assertEquals(10L, chunk.getCount());
        Assertions.assertEquals(10, chunk.getLast());
        Assertions.assertEquals("test", chunk.getOrigin());
        Assertions.assertEquals(false, chunk.isOutlier());
        Assertions.assertEquals(true, chunk.isTrend());
        Assertions.assertEquals(3.0276503540974917, chunk.getStdDev());
        Assertions.assertEquals("aabcddefgg", chunk.getSax());
        Assertions.assertEquals(5.5, chunk.getAvg());
        Assertions.assertEquals(SchemaVersion.VERSION_1, chunk.getVersion());
        Assertions.assertEquals("1970-01-01", chunk.getDay());
        //    Assertions.assertEquals("2c6191ea9a9abe5443b73da0c3c072819d5b6e6f2d2c379513067356e2dae019",chunk.getId());
        Assertions.assertEquals(1, chunk.getFirst());
        Assertions.assertEquals(10, chunk.getMax());
        Assertions.assertEquals(1, chunk.getMin());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals("metric 1", chunk.getName());
        Assertions.assertEquals(1, chunk.getStart());
        Assertions.assertEquals(55, chunk.getSum());
        Assertions.assertEquals(1111, chunk.getEnd());
        Assertions.assertEquals(tags, chunk.getTags());
        Assertions.assertEquals(1970, chunk.getYear());

        //        truncated chunk
        Chunk truncatedChunk = chunk.truncate(301, 799);
        Assertions.assertEquals(2, truncatedChunk.getCount());
        Assertions.assertEquals(7, truncatedChunk.getLast());
        //  Assertions.assertEquals(Collections.emptyList(), truncatedChunk.getCompactionRunnings());
        Assertions.assertEquals("ChunkTruncater", truncatedChunk.getOrigin());
      //  Assertions.assertEquals(Collections.emptyList(), truncatedChunk.getCompactionRunnings());
        Assertions.assertEquals("ChunkTruncater", truncatedChunk.getOrigin());
        Assertions.assertEquals(false, truncatedChunk.isOutlier());
        Assertions.assertEquals(true, truncatedChunk.isTrend());
        Assertions.assertEquals(0.7071067811865476, truncatedChunk.getStdDev());
        Assertions.assertEquals("ab", truncatedChunk.getSax());
        Assertions.assertEquals(6.5, truncatedChunk.getAvg());
        Assertions.assertEquals(SchemaVersion.VERSION_1, truncatedChunk.getVersion());
        Assertions.assertEquals("1970-01-01", truncatedChunk.getDay());
        //     Assertions.assertEquals("e5fc729560a76e72c439d9c614cf77ce876dc2da0175f563a542d8d5c90383a3",truncatedChunk.getId());
        Assertions.assertEquals(6, truncatedChunk.getFirst());
        Assertions.assertEquals(7, truncatedChunk.getMax());
        Assertions.assertEquals(6, truncatedChunk.getMin());
        Assertions.assertEquals(1, truncatedChunk.getMonth());
        Assertions.assertEquals("metric 1", truncatedChunk.getName());
        Assertions.assertEquals(500, truncatedChunk.getStart());
        Assertions.assertEquals(13, truncatedChunk.getSum());
        Assertions.assertEquals(600, truncatedChunk.getEnd());
        Assertions.assertEquals(tags, truncatedChunk.getTags());
        Assertions.assertEquals(1970, truncatedChunk.getYear());
    }

    @Test
    public void testDatesInUtc() {
        MeasuresToChunk converter = new MeasuresToChunkVersionCurrent("test");
        Chunk chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(0, 1)
                ))
        );
        Assertions.assertEquals("1970-01-01", chunk.getDay());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals(1970, chunk.getYear());

        chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(24l * 60l * 60l * 1000l - 1l, 1)
                ))
        );
        Assertions.assertEquals("1970-01-01", chunk.getDay());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals(1970, chunk.getYear());

        chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(24l * 60l * 60l * 1000l, 1)
                ))
        );
        Assertions.assertEquals("1970-01-02", chunk.getDay());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals(1970, chunk.getYear());

        chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(2l * 24l * 60l * 60l * 1000l - 1l, 1)
                ))
        );
        Assertions.assertEquals("1970-01-02", chunk.getDay());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals(1970, chunk.getYear());

        chunk = converter.buildChunk("metric 1",
                new TreeSet<Measure>(Arrays.asList(
                        Measure.fromValue(2l * 24l * 60l * 60l * 1000l, 1)
                ))
        );
        Assertions.assertEquals("1970-01-03", chunk.getDay());
        Assertions.assertEquals(1, chunk.getMonth());
        Assertions.assertEquals(1970, chunk.getYear());
    }


    Measure randomMeasure(String name, Map<String,String> tags) {
        DateTime time = new DateTime(1977,3,2,2,13)
                .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))));
        String day = time.toString("yyyy-MM-dd");

        long newTimestamp = time.getMillis() + (long) (Math.random() * 1000L);
        return Measure.builder()
                .name(name)
                .value(Math.random())
                .quality( (float) (Math.random() * 100.0f))
                .timestamp(newTimestamp)
                .tags(tags)
                .build();
    }

    @Test
    public void testConversion() {
        // build a bunch of random measures
        String name = "cpu";
        Map<String, String> tags = new HashMap<String, String>() {{
            put("couNtry", "France");
            put("usine", "usine 2 ;;Alpha go");
        }};
        TreeSet<Measure> inputMeasures = new TreeSet<>();
        for(int i=0; i<100; i++)
            inputMeasures.add(randomMeasure(name, tags));

        // convert them as a Chunk
        MeasuresToChunk converter = new MeasuresToChunkVersionCurrent("test");
        Chunk chunk = converter.buildChunk(name, inputMeasures, tags);

        // convert back to measures
        ChunkToMeasures chunkToMeasures = new ChunkToMeasuresConverter();
        TreeSet<Measure> outputMeasures = chunkToMeasures.buildMeasures(chunk);

        // check if this is the same
        Assertions.assertEquals(new ArrayList<>(inputMeasures), new ArrayList<>(outputMeasures));
    }
}
