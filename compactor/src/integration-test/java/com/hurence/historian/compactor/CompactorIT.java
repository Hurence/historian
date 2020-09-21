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
import com.hurence.timeseries.core.ChunkOrigin;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.unit5.extensions.SparkExtension;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        logger.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        logger.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN);
    }

    public static SolrInjector buildInjector() throws IOException {
        GeneralInjectorCurrentVersion chunkInjector = new GeneralInjectorCurrentVersion();
        Chunk chunk1 = ChunkBuilderHelper.fromPoints("metric",
                Arrays.asList(
                        Measure.fromValue(1000, 1),
                        Measure.fromValue(1000000, 2),
                        Measure.fromValue(10000000, 3),//1970-01-01T02:46:40.000Z   10000000
                        Measure.fromValue(150000000, 4),//1970-01-02T17:40:00.000Z  150000000
                        Measure.fromValue(200000000, 5)
                )
        );
        Chunk chunk2 = ChunkBuilderHelper.fromPoints("metric",
                Arrays.asList(
                        Measure.fromValue(200500000, 1),
                        Measure.fromValue(300000000, 2),
                        Measure.fromValue(400000000, 3),//1970-01-05T15:06:40.000Z
                        Measure.fromValue(500000000, 4),
                        Measure.fromValue(600000000, 5)
                )
        );
        chunkInjector.addChunk(chunk1);
        chunkInjector.addChunk(chunk2);
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
        Compactor compactor = new Compactor(compactorConfig);
        compactor.start();

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
        options.put("tag_names", "metric_id,min,max,warn,crit");
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
        options.put("tag_names", "metric_id");
        // JavaConverters used to convert from java Map to scala immutable Map
        sqlOptions = new Options(collectionName, JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(
                Predef.<Tuple2<String, String>>conforms()));
        Dataset<Chunk> chunks = (Dataset<Chunk>) solrChunksReader.read(sqlOptions)
                .where("metric_id LIKE '08%'");

        chunks.show(100, false);

        assertTrue(chunks.count() == 5);

        // Unchunkify and display matching metrics
        UnChunkyfier unchunkyfier = new UnChunkyfier();
        unchunkyfier.transform(chunks).show();
    }

    private static void printSolrJsonDocs(SolrClient solrClient, String collection) {

        SolrDocumentList solrDocumentList = getSolrDocs(solrClient, collection);

        System.out.println(JSONUtil.toJSON(solrDocumentList));
    }

    private static SolrDocumentList getSolrDocs(SolrClient solrClient, String collection) {
        SolrParams solrQuery = new SolrQuery("*:*");
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
