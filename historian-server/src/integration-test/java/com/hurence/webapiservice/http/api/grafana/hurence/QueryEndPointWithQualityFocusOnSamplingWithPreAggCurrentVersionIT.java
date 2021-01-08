package com.hurence.webapiservice.http.api.grafana.hurence;


import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.*;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointWithQualityFocusOnSamplingWithPreAggCurrentVersionIT {
    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointWithQualityFocusOnSamplingWithPreAggCurrentVersionIT.class);

    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        assertHelper = new AssertResponseGivenRequestHelper(HttpITHelper.buildWebClient(vertx), HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT);
    }

    @AfterAll
    public static void closeVertx(Vertx vertx, VertxTestContext context) {
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        initVerticles(container, vertx, context);
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initSolr(DockerComposeContainer container) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
    }

    public static void initVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        JsonObject historianConf = buildHistorianConf();
        HttpWithHistorianSolrITHelper
                .deployHttpAndCustomHistorianVerticle(container, vertx, historianConf)
                .subscribe(id -> {
                            context.completeNow();
                        },
                        t -> context.failNow(t));
    }

    public static JsonObject buildHistorianConf() {
        return new JsonObject()
                //10 so if more than 5 chunk (of size 2) returned we should sample
                //with pre aggs
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 10L)
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L);
    }

    public static SolrInjector buildInjector() {
        List<List<Measure>> pointsByChunk10Chunks = Arrays.asList(
                Arrays.asList(
                        Measure.fromValueAndQuality( 1L, 1.0, 0.9f),
                        Measure.fromValueAndQuality( 2L, 1.0, 0.8f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 3L, 2.0, 0.7f),
                        Measure.fromValueAndQuality( 4L, 2.0, 0.7f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 5L, 3.0, 0.8f),
                        Measure.fromValueAndQuality( 6L, 3.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 7L, 4.0, 0.7f),
                        Measure.fromValueAndQuality( 8L, 4.0, 0.7f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 9L, 5.0, 0.85f),
                        Measure.fromValueAndQuality( 10L, 5.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 11L, 6.0, 0.8f),
                        Measure.fromValueAndQuality( 12L, 6.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 13L, 7.0, 0.9f),
                        Measure.fromValueAndQuality( 14L, 7.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 15L, 8.0, 0.9f),
                        Measure.fromValueAndQuality( 16L, 8.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 17L, 9.0, 0.8f),
                        Measure.fromValueAndQuality( 18L, 9.0, 0.9f)
                ),
                Arrays.asList(
                        Measure.fromValueAndQuality( 19L, 10.0, 0.9f),
                        Measure.fromValueAndQuality( 20L, 10.0, 0.9f)
                )
        );
        AbstractSolrInjectorChunkCurrentVersion injector10chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_10_chunk", pointsByChunk10Chunks);
        AbstractSolrInjectorChunkCurrentVersion injector9chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_9_chunk", pointsByChunk10Chunks.stream().limit(9).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector7chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_7_chunk", pointsByChunk10Chunks.stream().limit(7).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector5chunk = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_5_chunk", pointsByChunk10Chunks.stream().limit(5).collect(Collectors.toList()));
        AbstractSolrInjectorChunkCurrentVersion injector1chunkOf20Point = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_1_chunk_of_20_points",
                Arrays.asList(
                        pointsByChunk10Chunks.stream().flatMap(List::stream).collect(Collectors.toList())
                )
        );
        injector10chunk.addChunk(injector9chunk);
        injector10chunk.addChunk(injector7chunk);
        injector10chunk.addChunk(injector5chunk);
        injector10chunk.addChunk(injector1chunkOf20Point);
        return injector10chunk;
    }

    /**
     * QUERY MODE 1 because
     * metricsInfo.getTotalNumberOfPointsWithCorrectQuality() <= getSamplingConf(request).getMaxPoint()
     * metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint()
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax20(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric10ChunkMaxPoint20/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric10ChunkMaxPoint20/expectedResponse.json");
    }

    /**
     * QUERY MODE 2
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric10ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric10ChunkMaxPoint4/expectedResponse.json");
    }

    /**
     * QUERY MODE 2
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric9ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric9ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric9ChunkMaxPoint4/expectedResponse.json");
    }

    /**
     * QUERY MODE 2
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric7ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric7ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric7ChunkMaxPoint4/expectedResponse.json");
    }

    /**
     * QUERY MODE 1 because :
     * metricsInfo.getTotalNumberOfPointsWithCorrectQuality() < solrHistorianConf.limitNumberOfPoint
     * metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint()
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric5ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric5ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric5ChunkMaxPoint4/expectedResponse.json");
    }

    /**
     * QUERY MODE 1 because :
     * metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint()
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric1ChunkOf20PointMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric1ChunkOf20PointMaxPoint4/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testMetric1ChunkOf20PointMaxPoint4/expectedResponse.json");
    }

    /*
        TEST testQualityAlgoFiltering => testFilteringQualityWithAvg
     */
/*    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringAvgAndSamplingAvg(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingAvg/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingAvg/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringAvgAndSamplingFirst(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingFirst/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingFirst/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringAvgAndSamplingMax(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingMax/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingMax/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringAvgAndSamplingMin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingMin/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithAvg/samplingMin/expectedResponse.json");
    }
    *//*
       TEST testQualityAlgoFiltering => testFilteringQualityWithFirst
    *//*
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringFirstAndSamplingAvg(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingAvg/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingAvg/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringFirstAndSamplingFirst(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingFirst/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingFirst/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringFirstAndSamplingMax(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingMax/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingMax/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringFirstAndSamplingMin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingMin/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithFirst/samplingMin/expectedResponse.json");
    }
    *//*
       TEST testQualityAlgoFiltering => testFilteringQualityWithMax
    *//*
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMaxAndSamplingAvg(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingAvg/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingAvg/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMaxAndSamplingFirst(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingFirst/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingFirst/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMaxAndSamplingMax(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingMax/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingMax/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMaxAndSamplingMin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingMin/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMax/samplingMin/expectedResponse.json");
    }
    *//*
       TEST testQualityAlgoFiltering => testFilteringQualityWithMin
    *//*
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMinAndSamplingAvg(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingAvg/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingAvg/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMinAndSamplingFirst(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingFirst/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingFirst/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMinAndSamplingMax(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingMax/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingMax/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQualityAlgoFilteringMinAndSamplingMin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingMin/request.json",
                "/http/grafana/hurence/query/testWithQualityWithPreAggSampling/testQualityAlgoFiltering/testFilteringQualityWithMin/samplingMin/expectedResponse.json");
    }*/


    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }
}
