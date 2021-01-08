package com.hurence.timeseries.analysis;

import com.hurence.timeseries.analysis.clustering.ChunkClusterable;
import com.hurence.timeseries.analysis.clustering.ChunksClustering;
import com.hurence.timeseries.analysis.clustering.KMeansChunksClustering;
import com.hurence.timeseries.converter.MeasuresToChunk;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.ChunkWrapper;
import com.hurence.timeseries.model.Measure;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    /*    for (int i = 0; i < 10; i++)
            chunks.add(randomChunk(name, tags5, 1440, Mode.NOISE));*/


        ChunksClustering clustering = KMeansChunksClustering.builder()
                .k(5).maxIterations(100)
                .distance(KMeansChunksClustering.Distance.DEFAULT)
                .build();

        List<ChunkClusterable> chunkWrappers = chunks.stream()
                .map(c -> new ChunkWrapper(c.getId(), c.getSax(), c.getTags()))
                .collect(Collectors.toList());


        AtomicInteger testPassCount = new AtomicInteger(4 * 1000);
        for (int i = 0; i < 100; i++) {
            clustering.cluster(chunks);
            AtomicReference<String> saxClusterForDChunk = new AtomicReference<>();
            Map<String, Integer> countByCluster = new HashMap<>();

            clustering.cluster(chunkWrappers);

            chunkWrappers.forEach(c -> {
             //   logger.info(c.getSax() + " --- " + c.getTags().toString());
                int prevValue = countByCluster.getOrDefault(c.getTags().get("sax_cluster"), 0);
                countByCluster.put(c.getTags().get("sax_cluster"), prevValue + 1);
            });

            countByCluster.values().forEach(v -> {
                if(v != 10)
                    testPassCount.addAndGet(-v);
            });
        }


        double perentPass = testPassCount.get() / 40.0;
        logger.info("clustering prediction ratio for typed random chunks : " + perentPass + "%");
        Assert.assertTrue(perentPass >= 95);
    }


    @Test
    public void tesAcrossDays() {

        List<ChunkClusterable> chunks = Arrays.asList(
                new ChunkClusterable[]{
                        new ChunkWrapper("a",
                                "abbbabdddddddddbbbcc",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-25");
                                }}),
                        new ChunkWrapper("b",
                                "ccbbbbdeddedddccbaaa",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-26");
                                }}),
                        new ChunkWrapper("c",
                                "cbcbbcddddddddddccca",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-27");
                                }}),
                        new ChunkWrapper("d",
                                "bbbbbbbdeecccccccccc",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-28");
                                }}),
                        new ChunkWrapper("e",
                                "aabbacdeeeeddddcbbba",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-29");
                                }}),
                        new ChunkWrapper("f",
                                "abbabcddeeedcdddbbbb",
                                new HashMap<String, String>() {{
                                    put("chunk_day", "2019-11-30");
                                }})

                }
        );

        ChunksClustering clustering = KMeansChunksClustering.builder()
                .k(4).maxIterations(100)
                .distance(KMeansChunksClustering.Distance.DEFAULT)
                .build();

        // run 10000 times to see big scale prediction ratio
        int testPassCount = 0;
        for (int i = 0; i < 10000; i++) {
            clustering.cluster(chunks);
            AtomicReference<String> saxClusterForDChunk = new AtomicReference<>();
            Map<String, Integer> countByCluster = new HashMap<>();
            chunks.forEach(c -> {
                int prevValue = countByCluster.getOrDefault(c.getTags().get("sax_cluster"), 0);
                countByCluster.put(c.getTags().get("sax_cluster"), prevValue + 1);
                if (c.getId().equals("d"))
                    saxClusterForDChunk.set(c.getTags().get("sax_cluster"));
            });

            if (countByCluster.get(saxClusterForDChunk.get()) == 1) {
                testPassCount++;
            }

        }
        logger.info("clustering prediction ratio for known sax strings : " + testPassCount / 100 + "%");
        Assert.assertTrue(testPassCount / 100 >= 95);
    }

}
