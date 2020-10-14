package com.hurence.timeseries.analysis;

import com.hurence.timeseries.analysis.clustering.ChunkClusterable;
import com.hurence.timeseries.analysis.clustering.ChunksClustering;
import com.hurence.timeseries.analysis.clustering.KMeansChunksClustering;
import com.hurence.timeseries.converter.MeasuresToChunk;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChunksClustererTest {

    private static Logger logger = LoggerFactory.getLogger(ChunksClustererTest.class.getName());

    enum Mode {
        NOISE,
        TREND_UP,
        TREND_DOWN,
        CONSTANT,
        SQUARE,
        SINUSOID
    }


    private Measure randomMeasure(String name, Map<String, String> tags, Mode mode, long newTimestamp, double increment, boolean halfArray) {


        float newQuality = (float) (Math.random() * 100.0f);
        double newValue = Math.random();

        switch (mode) {
            case CONSTANT:
                newValue = Math.PI;
                break;
            case SQUARE:
                if (halfArray)
                    newValue = Math.PI;
                else
                    newValue = Math.PI / 2;
                break;
            case NOISE:
                newValue = Math.random();
                break;
            case TREND_UP:
                newValue = newTimestamp;
                break;
            case SINUSOID:
                newValue = Math.sin(increment);
                break;
        }

        return Measure.builder()
                .name(name)
                .value(newValue)
                .quality(newQuality)
                .timestamp(newTimestamp)
                .tags(tags)
                .build();
    }


    private Chunk randomChunk(String name, Map<String, String> tags, int nbMeasures, Mode mode) {

        DateTimeZone timeZone = DateTimeZone.forID("UTC");
        DateTime today = new DateTime(timeZone).withTimeAtStartOfDay();
        DateTime tomorrow = today.plusDays(1).withTimeAtStartOfDay();

        long start = today.getMillis();
        long end = tomorrow.getMillis() - 1;
        long delta = (long) ((end - start) / (double) nbMeasures);
        double sinIncrement = (2 * Math.PI) / (double) nbMeasures;

        long chunkCurrentTime = start;
        double currentSinValue = 0;

        boolean halfArray = false;
        TreeSet<Measure> inputMeasures = new TreeSet<>();
        for (int i = 0; i < nbMeasures; i++) {
            if (i > nbMeasures / 2)
                halfArray = true;
            inputMeasures.add(randomMeasure(name, tags, mode, chunkCurrentTime, currentSinValue, halfArray));
            chunkCurrentTime += delta;
            currentSinValue += sinIncrement;
        }

        // convert them as a Chunk
        MeasuresToChunk converter = new MeasuresToChunkVersionCurrent("test");
        return converter.buildChunk(name, inputMeasures, tags);
    }

    @Test
    public void tesKMeans() {

        String name = "cpu";
        Map<String, String> tags1 = new HashMap<String, String>() {{
            put("gen", "CONSTANT");
        }};
        Map<String, String> tags2 = new HashMap<String, String>() {{
            put("gen", "SINUSOID");
        }};
        Map<String, String> tags3 = new HashMap<String, String>() {{
            put("gen", "TREND_UP");
        }};

        Map<String, String> tags4 = new HashMap<String, String>() {{
            put("gen", "SQUARE");
        }};
        Map<String, String> tags5 = new HashMap<String, String>() {{
            put("gen", "NOISE");
        }};

        List<ChunkClusterable> chunks = new ArrayList<>();


        for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags1, 1440, Mode.CONSTANT));
        for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags2, 1440, Mode.SINUSOID));
        for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags3, 1440, Mode.TREND_UP));
        for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags4, 1440, Mode.SQUARE));
        for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags5, 1440, Mode.NOISE));




        ChunksClustering clustering = KMeansChunksClustering.builder()
                .k(5).maxIterations(10)
                .distance(KMeansChunksClustering.Distance.DEFAULT)
                .build();

        clustering.cluster(chunks);

        chunks.forEach(c -> logger.info(c.getSax() + " --- " + c.getTags().toString()));
    }
}
