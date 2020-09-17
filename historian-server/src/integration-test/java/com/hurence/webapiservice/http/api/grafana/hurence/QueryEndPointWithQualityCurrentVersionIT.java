package com.hurence.webapiservice.http.api.grafana.hurence;


import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
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
import java.util.concurrent.TimeUnit;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointWithQualityCurrentVersionIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointWithQualityCurrentVersionIT.class);

    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        assertHelper = new AssertResponseGivenRequestHelper(HttpITHelper.buildWebClient(vertx), HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
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
        SolrInjector injector = new SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion(
                Arrays.asList("temp_a", "temp_b", "temp_c", "mixed1", "mixed2", "non_mixed"),
                Arrays.asList(
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 622, 0.9f),
                                Measure.fromValueAndQuality( 1477916224866L, -3, 0.8f),
                                Measure.fromValueAndQuality( 1477917224866L, 365, 0.7f),      // avg = 0.75
                                Measure.fromValueAndQuality( 1477924624866L, 568, 0.6f),
                                Measure.fromValueAndQuality( 1477948224866L, 14, 0.8f),
                                Measure.fromValueAndQuality( 1477957224866L, 86, 0.7f)
                        ),//temp_b
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 861, 0.8f),
                                Measure.fromValueAndQuality( 1477917224866L, 767, 0.9f),
                                Measure.fromValueAndQuality( 1477927624866L, 57, 0.7f),
                                Measure.fromValueAndQuality( 1477931224866L, 125, 0.6f),    // avg = 0.783
                                Measure.fromValueAndQuality( 1477945624866L, 710, 0.8f),
                                Measure.fromValueAndQuality( 1477985224866L, 7, 0.9f)
                        ),//temp_c
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 861, 0.8f),
                                Measure.fromValueAndQuality( 1477917224866L, 767, 0.8f),
                                Measure.fromValueAndQuality( 1477927624866L, 57, 0.8f),     // avg = 0.8
                                Measure.fromValueAndQuality( 1477931224866L, 125, 0.8f),
                                Measure.fromValueAndQuality( 1477945624866L, 710, 0.8f),
                                Measure.fromValueAndQuality( 1477985224866L, 7, 0.8f)
                        ),//mixed1
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 861, 0.8f),
                                Measure.fromValue( 1477917224866L, 767),
                                Measure.fromValueAndQuality( 1477927624866L, 57, 0.7f),     //avg = 0.81
                                Measure.fromValueAndQuality( 1477931224866L, 125, 0.6f),
                                Measure.fromValueAndQuality( 1477945624866L, 710, 0.8f),
                                Measure.fromValue( 1477985224866L, 7)
                        ),//mixed2
                        Arrays.asList(
                                Measure.fromValue( 1477895624866L, 622),
                                Measure.fromValueAndQuality( 1477916224866L, -3, 0.4f),
                                Measure.fromValueAndQuality( 1477917224866L, 365, 0.7f),
                                Measure.fromValueAndQuality( 1477924624866L, 568, 0.6f),     // avg = 0.65
                                Measure.fromValue( 1477948224866L, 14),
                                Measure.fromValueAndQuality( 1477957224866L, 86, 0.2f)
                        ),//non_mixed
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 622, 0.8f),
                                Measure.fromValueAndQuality( 1477916224866L, -3, 0.4f),
                                Measure.fromValueAndQuality( 1477917224866L, 365, 0.7f),    // avg = 0.58
                                Measure.fromValueAndQuality( 1477924624866L, 568, 0.6f),
                                Measure.fromValueAndQuality( 1477948224866L, 14, 0.8f),
                                Measure.fromValueAndQuality( 1477957224866L, 86, 0.2f)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
    }

    public static void initVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQuality(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test1/request.json",
                "/http/grafana/hurence/query/testWithQuality/test1/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithoutQuality(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test2/request.json",
                "/http/grafana/hurence/query/testWithQuality/test2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityForEachMetric(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test3/request.json",
                "/http/grafana/hurence/query/testWithQuality/test3/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityWithMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test4/request.json",
                "/http/grafana/hurence/query/testWithQuality/test4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithoutQualityWithMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test5/request.json",
                "/http/grafana/hurence/query/testWithQuality/test5/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
