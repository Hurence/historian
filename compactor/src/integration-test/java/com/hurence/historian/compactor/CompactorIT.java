package com.hurence.historian.compactor;

import com.hurence.historian.compactor.config.Configuration;
import com.hurence.historian.compactor.config.ConfigurationBuilder;
import com.hurence.historian.compactor.config.ConfigurationException;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.historian.spark.ml.Chunkyfier;
import com.hurence.historian.spark.ml.UnChunkyfier;
import com.hurence.historian.spark.sql.Options;
import com.hurence.historian.spark.sql.reader.ChunksReaderType;
import com.hurence.historian.spark.sql.reader.ReaderFactory;
import com.hurence.historian.spark.sql.reader.solr.SolrChunksReader;
import com.hurence.historian.spark.sql.writer.WriterFactory;
import com.hurence.historian.spark.sql.writer.WriterType;
import com.hurence.historian.spark.sql.writer.solr.SolrChunksWriter;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.core.ChunkOrigin;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.unit5.extensions.SparkExtension;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.spark.sql.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.noggit.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.timeseries.model.HistorianChunkCollectionFieldsVersionCurrent.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    public static void beforeAll(SolrClient client, DockerComposeContainer container) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_1);

        // Add fields for tags
        SolrITHelper.addFieldToChunkSchema(container, "dataCenter");
        SolrITHelper.addFieldToChunkSchema(container, "room");
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        logger.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        logger.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);
    }

    public static SolrInjector buildInjector() throws IOException {
        GeneralInjectorCurrentVersion chunkInjector = new GeneralInjectorCurrentVersion();

        /**
         * metric1 tags1 2020-08-28 from injector
         */

        Chunk chunk = ChunkBuilderHelper.fromPointsAndTags("metric1",
                Arrays.asList(
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:51.001", 11, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:52.002", 12, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:53.003", 13, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:54.004", 14, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:30:55.005", 15, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:51.006", 11, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:52.007", 12, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 17:40:53.008", 13, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:51.009", 11, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 16:10:52.010", 12, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:51.011", 21, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:52.012", 22, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:53.013", 23, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:54.014", 24, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 02:10:55.015", 25, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:51.016", 21, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:52.017", 22, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:10:53.018", 23, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:51.019", 21, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 01:05:52.020", 22, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.021", 31, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.022", 32, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.023", 33, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.024", 34, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.025", 35, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.026", 31, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.027", 32, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.028", 33, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.029", 31, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.030", 32, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:51.031", 41, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:52.032", 42, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:53.033", 43, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:54.034", 44, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:10:55.035", 45, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:51.036", 41, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:52.037", 42, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-27 12:20:53.038", 43, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:51.039", 41, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-27 11:20:52.040", 42, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:51.041", 51, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:52.042", 52, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:53.043", 53, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:54.044", 54, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:47:55.045", 55, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:51.046", 51, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:52.047", 52, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-28 21:57:53.048", 53, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:51.049", 51, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:57:52.050", 52, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:51.051", 61, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:52.052", 62, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:53.053", 63, 93),
                        Measure.fromValueAndQualityWithDate("2020-08-29 09:10:54.054", 64, 94),
                        Measure.fromValueAndQualityWithDate("2020-08-28 09:10:55.055", 65, 95)
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
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:51.056", 61, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:52.057", 62, 92),
                        Measure.fromValueAndQualityWithDate("2020-08-29 08:05:53.058", 63, 93)
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
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:51.059", 61, 91),
                        Measure.fromValueAndQualityWithDate("2020-08-29 06:05:52.060", 62, 92)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 11:10:55.055", 1055, 1095)
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
                        Measure.fromValueAndQualityWithDate("2020-08-28 22:10:55.055", 1060, 1095)
                ),
                new HashMap<String, String>() {{
                    put("dataCenter", "1");
                    put("room", "1");
                }}
                ,
                ChunkOrigin.COMPACTOR.toString()
        );
        chunkInjector.addChunk(chunk);

        return chunkInjector;
    }

    /**
     *
     */
    @Test
    public void testCompactor(DockerComposeContainer container, SolrClient solrClient, SparkSession sparkSession) {
//        public void testCompactor() {

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
        //compactor.start();

//        sparkSession.read().format("solr");

//        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
//        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

//        Map<String, String> options = new HashMap<String, String>();
//        options.put("zkhost", zkHost);
//        options.put("collection", SolrITHelper.COLLECTION_HISTORIAN);
//        options.put("query", queryStr);
//        options.put(ConfigurationConstants.SOLR_SPLIT_FIELD_PARAM(), "_version_");
//        options.put(ConfigurationConstants.SOLR_SPLITS_PER_SHARD_PARAM(), "4");
//        options.put(ConfigurationConstants.SOLR_FIELD_PARAM(), "id,_version_,"+UID_FIELD+","+TS_FIELD+",bytes_s,response_s,verb_s");

        // Use the Solr DataSource to load rows from a Solr collection using a query
        // highlights include:
        //   - parallelization of reads from each shard in Solr
        //   - more parallelization by splitting each shard into ranges
        //   - results are streamed back from Solr using deep-paging and streaming response
//        Dataset<Row> chunks = sparkSession.read().format("solr").options(options).load();

        printSolrJsonDocs(solrClient, SolrITHelper.COLLECTION_HISTORIAN);

        compactor.doCompact();

        System.out.println("After recompaction: ");
        printUnChunkedSolrJsonDocs(solrClient, SolrITHelper.COLLECTION_HISTORIAN);
        System.exit(1);

        // TODO: test recompaction changing the max number of values per chunk

        // 1. load measures from parquet file
        String filePath = new File("../loader/src/test/resources/it-data-4metrics.parquet").getAbsolutePath();

        Dataset<Row> measures = sparkSession.read()
                .parquet(filePath)
                .cache();
        measures.show();

//        Dataset<Row> measures = null;
//        String zkHost = null;
//        SolrClient solrClient = null;

        // 2. make chunks from measures
        Chunkyfier chunkyfier = new Chunkyfier()
                .setValueCol("value")
                .setQualityCol("quality")
                .setOrigin(ChunkOrigin.COMPACTOR.toString())
                .setTimestampCol("timestamp")
                .setGroupByCols(new String[]{"name", "tags.metric_id"})
                .setDateBucketFormat("yyyy-MM-dd")
                .setSaxAlphabetSize(7)
                .setSaxStringLength(50);
        Dataset<Row> ack08Rows = chunkyfier.transform(measures)
                .where("name = 'ack' AND avg != 0.0")
                .repartition(1);

        ack08Rows.show();

        Dataset<Chunk> ack08 = ack08Rows
                .as(Encoders.bean(Chunk.class));

        // 3. write those chunks to SolR
        SolrChunksWriter solrChunksWriter = (SolrChunksWriter)WriterFactory.getChunksWriter(WriterType.SOLR());
        Map<String, String> options = new HashMap<String, String>();
        options.put("zkhost", zkHost);
        String collectionName = SolrITHelper.COLLECTION_HISTORIAN;
        options.put("collection", collectionName);
        options.put(Options.TAG_NAMES(), "metric_id,min,max,warn,crit");
        // JavaConverters used to convert from java Map to scala immutable Map
        Options sqlOptions = new Options(collectionName, JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));
        solrChunksWriter.write(sqlOptions, ack08);

        // 4. Explicit commit to make sure all docs are visible
        try {
            solrClient.commit(collectionName, true, true);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        printSolrJsonDocs(solrClient, collectionName);

        // 5. load back those chunks to verify
        SolrChunksReader solrChunksReader = (SolrChunksReader)ReaderFactory.getChunksReader(ChunksReaderType.SOLR());
        options = new HashMap<String, String>();
        options.put("zkhost", zkHost);
        options.put("collection", collectionName);
        options.put(Options.TAG_NAMES(), "metric_id");
        // JavaConverters used to convert from java Map to scala immutable Map
        sqlOptions = new Options(collectionName, JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));
        Dataset<Chunk> chunks = (Dataset<Chunk>)solrChunksReader.read(sqlOptions)
                .where("metric_id LIKE '08%'");

        chunks.show(100, false);

        assertTrue(chunks.count() == 5);

        // Unchunkify and display matching metrics
        UnChunkyfier unchunkyfier = new UnChunkyfier();
        unchunkyfier.transform(chunks).show();
    }

    private static void printSolrJsonDocs(SolrClient solrClient, String collection) {

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        System.out.println("Solr contains " + solrDocumentList.size() +
                " document(s):\n" + JSONUtil.toJSON(solrDocumentList));
    }

    private static void printUnChunkedSolrJsonDocs(SolrClient solrClient, String collection) {

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        System.out.println("Solr contains " + solrDocumentList.size() +
                " document(s):");

        SimpleDateFormat sdf = Measure.createUtcDateFormatter("yyyy-MM-dd HH:mm:ss.SSS");
        StringBuilder stringBuilder = new StringBuilder("\n{");

        for (SolrDocument solrDocument : solrDocumentList) {
            System.out.println("docsssssssssss:" + solrDocument);
            for (String field : solrDocument.getFieldNames().stream().sorted().collect(Collectors.toList())) {
                stringBuilder.append("\n  \"").append(field).append("\": ");
                Object value = solrDocument.getFieldValue(field);
                if (field.equals(CHUNK_VALUE)) {
                    // Uncompress the chunk and display points in a human readable way
                    byte[] compressedPoints = BinaryEncodingUtils.decode((String)value);
                    long chunkStart = (Long)solrDocument.getFieldValue(CHUNK_START);
                    long chunkEnd = (Long)solrDocument.getFieldValue(CHUNK_END);
                    try {
                        TreeSet<Measure> measures = BinaryCompactionUtil.unCompressPoints(compressedPoints, chunkStart, chunkEnd);
                        for (Measure measure : measures) {
                            double measureValue = measure.getValue();
                            float quality = measure.getQuality();
                            String readableTimestamp = sdf.format(new Date(measure.getTimestamp()));
                            stringBuilder.append("\n    t=").append(readableTimestamp)
                                    .append(" v=").append(measureValue)
                                    .append(" q=").append(quality);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                } else
                {
                    stringBuilder.append(value);
                }
            }
            stringBuilder.append("\n}");
        }

        System.out.println(stringBuilder);
    }

    private static SolrDocumentList getSolrDocs(SolrClient solrClient, String collection) {
        SolrParams solrQuery = new SolrQuery("*:*").setRows(1000);
        QueryResponse queryResponse = null;
        try {
            queryResponse = solrClient.query(collection, solrQuery);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return queryResponse.getResults();
    }
}
