package com.hurence.historian.compactor;

import com.hurence.historian.compactor.config.Configuration;
import com.hurence.historian.compactor.config.ConfigurationBuilder;
import com.hurence.historian.compactor.config.ConfigurationException;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.core.ChunkOrigin;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.unit5.extensions.SparkExtension;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.sql.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.*;

import static com.hurence.historian.solr.util.SolrITHelper.*;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Compactor integrations tests.
 * To run me from the IDE:
 * - first build integration-tests module using the following command line from the root of the workspace:
 *   mvn -P build-integration-tests clean package
 * - then run this class from the IDE
 */
@ExtendWith({SolrExtension.class, SparkExtension.class})
public class CompactorIT {

    private static Logger logger = LoggerFactory.getLogger(CompactorIT.class);

    @BeforeAll
    public static void beforeAll(DockerComposeContainer container) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
    }

    @BeforeEach
    public void beforeEach(SolrClient client) {
        clearCollection(client, SolrITHelper.COLLECTION_HISTORIAN);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {

        // Create collection with schema
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_1);

        // Add fields for tags in test data
        SolrITHelper.addFieldToChunkSchema(container, "dataCenter");
        SolrITHelper.addFieldToChunkSchema(container, "room");
    }

    public static void injectChunksForTestCompactor(SolrClient client) {
        logger.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);

        GeneralInjectorCurrentVersion chunkInjector = new GeneralInjectorCurrentVersion();

        /**
         * metric1 tags1 2020-08-28 from injector
         */

        Chunk chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:51.001", 1, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.002", 2, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:53.003", 3, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:54.004", 4, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:55.005", 5, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:51.006", 6, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:52.007", 7, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:53.008", 8, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:51.009", 9, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:52.010", 10, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags1 2020-08-28 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:29:51.001", 1001, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.502", 1002, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:53.503", 1003, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:54.504", 1004, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:31:55.005", 1005, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 15:29:51.001", 1006, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:30:54.504", 1007, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.503", 1008, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:52.008", 1009, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 18:31:55.005", 1010, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags2 2020-08-28 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:51.011", 11, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:52.012", 12, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:53.013", 13, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:54.014", 14, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:55.015", 15, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:51.016", 16, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:52.017", 17, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:53.018", 18, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:51.019", 19, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:52.020", 20, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags2 2020-08-28 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:09:51.011", 1011, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:52.112", 1012, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:53.113", 1013, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:54.114", 1014, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:11:55.015", 1015, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:06:51.011", 1016, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:06:52.112", 1017, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:12:53.113", 1018, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:15:54.114", 1019, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:15:55.015", 1020, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags1 2020-08-27 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.021", 21, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.022", 22, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.023", 23, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.024", 24, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.025", 25, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.026", 26, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.027", 27, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.028", 28, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.029", 29, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.030", 30, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags1 2020-08-27 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.121", 1021, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.122", 1022, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.123", 1023, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.124", 1024, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:56.025", 1025, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:10:52.121", 1026, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:13:52.122", 1027, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:13:54.123", 1028, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:25:54.124", 1029, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:25:56.025", 1030, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags3 2020-08-27 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.031", 31, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.032", 32, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.033", 33, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.034", 34, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.035", 35, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.036", 36, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.037", 37, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.038", 38, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.039", 39, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.040", 40, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags3 2020-08-27 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:51.031", 1031, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:52.032", 1032, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:53.033", 1033, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:54.034", 1034, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:55.035", 1035, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:51.031", 1036, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:52.032", 1037, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:53.033", 1038, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:54.034", 1039, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:55.035", 1040, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric2 tags1 2020-08-28 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:51.041", 41, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:52.042", 42, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:53.043", 43, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:54.044", 44, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:55.045", 45, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:51.046", 46, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:52.047", 47, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:53.048", 48, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:51.049", 49, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:52.050", 50, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric2 tags1 2020-08-28 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:07:51.041", 1041, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:17:52.042", 1042, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:27:53.043", 1043, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:37:54.044", 1044, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:55.045", 1045, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:07:51.041", 1046, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:17:52.042", 1047, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:27:53.043", 1048, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:37:54.044", 1049, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:57:55.045", 1050, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags1 2020-08-29 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:51.051", 51, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:52.052", 52, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:53.053", 53, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:54.054", 54, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:55.055", 55, 95)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:51.056", 56, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:52.057", 57, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:53.058", 58, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:51.059", 59, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:52.060", 60, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric1 tags1 2020-08-29 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 05:10:51.051", 1051, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:10:52.052", 1052, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:30:53.053", 1053, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:10:54.054", 1054, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 11:10:55.055", 1055, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:10:51.051", 1056, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:10:52.052", 1057, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:30:53.053", 1058, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 21:10:54.054", 1059, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 22:10:55.055", 1060, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric0 no tags 2020-08-27 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 06:08:41.001", 61, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 07:08:41.001", 62, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 08:08:41.001", 63, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 09:08:41.001", 64, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 10:08:41.001", 65, 91)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 01:08:41.001", 66, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 02:08:41.001", 67, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 03:08:41.001", 68, 91)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:08:41.001", 69, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 13:08:41.001", 70, 92)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric0 no tags 2020-08-27 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 05:10:51.051", 1061, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 07:10:52.052", 1062, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 08:30:53.053", 1063, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 10:10:54.054", 1064, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:10:55.055", 1065, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 01:10:51.051", 1066, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 02:10:52.052", 1067, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 03:30:53.053", 1068, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 21:10:54.054", 1069, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 22:10:55.055", 1070, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric0 no tags 2020-08-29 from injector
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:02:57.001", 71, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:02:57.001", 72, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:02:57.001", 73, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:02:57.001", 74, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:02:57.001", 75, 91)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:02:57.001", 76, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:02:57.001", 77, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:02:57.001", 78, 91)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 12:02:57.001", 79, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 13:02:57.001", 80, 92)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.INJECTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        /**
         * metric0 no tags 2020-08-29 from compactor
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 05:11:51.051", 1071, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:11:52.052", 1072, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:31:53.053", 1073, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:11:54.054", 1074, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 11:11:55.055", 1075, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);
        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:11:51.051", 1076, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:11:52.052", 1077, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:31:53.053", 1078, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 21:11:54.054", 1079, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 22:11:55.055", 1080, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        try {
            chunkInjector.injectChunks(client);
        } catch (Exception e) {
            fail("Error injecting chunks into Solr: " + e.getMessage());
        }

        logger.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);
    }

    private static Map<String, Chunk> expectedRecompactedChunksForTestCompactor() {

        Map<String, Chunk> expectedChunks = new HashMap<String, Chunk>();

        /**
         * metric1 tags1 2020-08-28
         */

        Chunk chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 15:29:51.001", 1006, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:51.009", 9, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:52.010", 10, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:30:54.504", 1007, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:29:51.001", 1001, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:51.001", 1, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.002", 2, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.502", 1002, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.503", 1008, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:53.003", 3, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:53.503", 1003, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:54.004", 4, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:54.504", 1004, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:55.005", 5, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:31:55.005", 1005, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:51.006", 6, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:52.007", 7, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:52.008", 1009, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:53.008", 8, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 18:31:55.005", 1010, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric1 tags2 2020-08-28
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:51.019", 19, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:52.020", 20, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:06:51.011", 1016, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:06:52.112", 1017, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:51.016", 16, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:52.017", 17, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:53.018", 18, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:12:53.113", 1018, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:09:51.011", 1011, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:51.011", 11, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:52.012", 12, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:52.112", 1012, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:53.013", 13, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:53.113", 1013, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:54.014", 14, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:54.114", 1014, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:55.015", 15, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:11:55.015", 1015, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:15:54.114", 1019, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:15:55.015", 1020, 1095)

                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "2");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric1 tags1 2020-08-27
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:10:52.121", 1026, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.029", 29, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.030", 30, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.021", 21, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.022", 22, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.121", 1021, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.122", 1022, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.023", 23, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.024", 24, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.123", 1023, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.124", 1024, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.025", 25, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:56.025", 1025, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.026", 26, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.027", 27, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.028", 28, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:13:52.122", 1027, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:13:54.123", 1028, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:25:54.124", 1029, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:25:56.025", 1030, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric1 tags3 2020-08-27
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.039", 39, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.040", 40, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.031", 31, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.032", 32, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.033", 33, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.034", 34, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.035", 35, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:51.031", 1031, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:52.032", 1032, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:53.033", 1033, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:54.034", 1034, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:11:55.035", 1035, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:51.031", 1036, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:52.032", 1037, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:53.033", 1038, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:54.034", 1039, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:12:55.035", 1040, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.036", 36, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.037", 37, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.038", 38, 93)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric2 tags1 2020-08-28
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric2",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:07:51.041", 1046, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:17:52.042", 1047, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:27:53.043", 1048, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:37:54.044", 1049, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 05:57:55.045", 1050, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:07:51.041", 1041, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:17:52.042", 1042, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:27:53.043", 1043, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:37:54.044", 1044, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:51.041", 41, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:52.042", 42, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:53.043", 43, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:54.044", 44, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:55.045", 45, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:51.046", 46, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:52.047", 47, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:53.048", 48, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:55.045", 1045, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:51.049", 49, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:52.050", 50, 92)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "2");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric1 tags1 2020-08-29
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:10:51.051", 1056, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:10:52.052", 1057, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:30:53.053", 1058, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 05:10:51.051", 1051, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:51.059", 59, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:52.060", 60, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:10:52.052", 1052, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:51.056", 56, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:52.057", 57, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:53.058", 58, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:30:53.053", 1053, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:51.051", 51, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:52.052", 52, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:53.053", 53, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:54.054", 54, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:55.055", 55, 95),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:10:54.054", 1054, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 11:10:55.055", 1055, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-29 21:10:54.054", 1059, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 22:10:55.055", 1060, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric0 no tags 2020-08-27
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-27 01:08:41.001", 66, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 01:10:51.051", 1066, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 02:08:41.001", 67, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 02:10:52.052", 1067, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 03:08:41.001", 68, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 03:30:53.053", 1068, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 05:10:51.051", 1061, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-27 06:08:41.001", 61, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 07:08:41.001", 62, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 07:10:52.052", 1062, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-27 08:08:41.001", 63, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 08:30:53.053", 1063, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-27 09:08:41.001", 64, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 10:08:41.001", 65, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 10:10:54.054", 1064, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:10:55.055", 1065, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:08:41.001", 69, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 13:08:41.001", 70, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 21:10:54.054", 1069, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-27 22:10:55.055", 1070, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        /**
         * metric0 no tags 2020-08-29
         */

        chunk = ChunkBuilderHelper.fromPointsAndTags("metric0",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:02:57.001", 76, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 01:11:51.051", 1076, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:02:57.001", 77, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 02:11:52.052", 1077, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:02:57.001", 78, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 03:31:53.053", 1078, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 05:11:51.051", 1071, 1091),
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:02:57.001", 71, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:02:57.001", 72, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 07:11:52.052", 1072, 1092),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:02:57.001", 73, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:31:53.053", 1073, 1093),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:02:57.001", 74, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:02:57.001", 75, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 10:11:54.054", 1074, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 11:11:55.055", 1075, 1095),
                        Measure.fromValueAndQualityWithDate("2020-08-29 12:02:57.001", 79, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 13:02:57.001", 80, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 21:11:54.054", 1079, 1094),
                        Measure.fromValueAndQualityWithDate("2020-08-29 22:11:55.055", 1080, 1095)
                ),
                new HashMap<String, String>()
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        expectedChunks.put(chunk.getId(), chunk);

        return expectedChunks;
    }

    /**
     * Test compactor scenario
     */
    @Test
    public void testCompactor(DockerComposeContainer container, SolrClient solrClient, SparkSession sparkSession) {

        injectChunksForTestCompactor(solrClient);

        String chunksCollection = SolrITHelper.COLLECTION_HISTORIAN;
        String solrHost = SolrExtension.getSolr1Url(container);
        System.out.println("Solr1 url:" + solrHost);
        String zkHost = SolrExtension.getZkUrl(container);
        System.out.println("Zk url:" + zkHost);

        String compactorConfigPath = getClass().getResource("/compactor-test-simple.yaml").getPath();
        Configuration compactorConfig = null;
        try {
            compactorConfig = ConfigurationBuilder.load(compactorConfigPath, logger);
        } catch (ConfigurationException e) {
            logger.error("Error loading configuration file: " + e.getMessage());
            fail("Could not load compactor config file: " + compactorConfigPath);
        }

        compactorConfig.setSolrZkHost(zkHost);
        logger.info("Using compactor configuration: " + compactorConfig);
        Compactor compactor = new Compactor(compactorConfig);

//        System.out.println("Before recompaction: ");
//        printSolrJsonDocs(solrClient, chunksCollection);

        compactor.doCompact();

//        System.out.println("After recompaction: ");
//        printUnChunkedSolrJsonDocs(solrClient, chunksCollection);

        compactor.close();

        Map<String, Chunk> actualChunks = getSolrChunks(solrClient, chunksCollection);

//        for (Chunk actualChunk : actualChunks.values()) {
//            System.out.println("Actual chunk:\n" + actualChunk.toHumanReadable());
//        }

        Map<String, Chunk> expectedChunks = expectedRecompactedChunksForTestCompactor();

//        for (Chunk expectedChunk : expectedChunks.values()) {
//            System.out.println("Expected chunk:\n" + expectedChunk.toHumanReadable());
//        }

        compareChunks(expectedChunks, actualChunks);
    }
}
