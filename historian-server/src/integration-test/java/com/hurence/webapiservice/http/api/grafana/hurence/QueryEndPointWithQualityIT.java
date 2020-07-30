package com.hurence.webapiservice.http.api.grafana.hurence;


import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.Version1SolrInjectorMultipleMetricSpecificPoints;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.modele.PointImpl;
import com.hurence.timeseries.modele.PointWithQualityImpl;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.api.grafana.GrafanaApiVersion;
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
import java.util.concurrent.TimeUnit;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointWithQualityIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointWithQualityIT.class);

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
        initSolrAndVerticles(client, container, vertx, context);
        injectChunksIntoSolr(client);
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new Version1SolrInjectorMultipleMetricSpecificPoints(
                Arrays.asList("temp_a", "temp_b", "temp_c", "mixed1", "mixed2", "non_mixed"),
                Arrays.asList(
                        Arrays.asList(
                                new PointWithQualityImpl( 1477895624866L, 622, 0.9f),
                                new PointWithQualityImpl( 1477916224866L, -3, 0.8f),
                                new PointWithQualityImpl( 1477917224866L, 365, 0.7f),
                                new PointWithQualityImpl( 1477924624866L, 568, 0.6f),
                                new PointWithQualityImpl( 1477948224866L, 14, 0.8f),
                                new PointWithQualityImpl( 1477957224866L, 86, 0.7f)
                        ),
                        Arrays.asList(
                                new PointWithQualityImpl( 1477895624866L, 861, 0.8f),
                                new PointWithQualityImpl( 1477917224866L, 767, 0.9f),
                                new PointWithQualityImpl( 1477927624866L, 57, 0.7f),
                                new PointWithQualityImpl( 1477931224866L, 125, 0.6f),
                                new PointWithQualityImpl( 1477945624866L, 710, 0.8f),
                                new PointWithQualityImpl( 1477985224866L, 7, 0.9f)
                        ),
                        Arrays.asList(
                                new PointWithQualityImpl( 1477895624866L, 861, 0.8f),
                                new PointWithQualityImpl( 1477917224866L, 767, 0.8f),
                                new PointWithQualityImpl( 1477927624866L, 57, 0.8f),
                                new PointWithQualityImpl( 1477931224866L, 125, 0.8f),
                                new PointWithQualityImpl( 1477945624866L, 710, 0.8f),
                                new PointWithQualityImpl( 1477985224866L, 7, 0.8f)
                        ),
                        Arrays.asList(
                                new PointWithQualityImpl( 1477895624866L, 861, 0.8f),
                                new PointImpl( 1477917224866L, 767),
                                new PointWithQualityImpl( 1477927624866L, 57, 0.7f),
                                new PointWithQualityImpl( 1477931224866L, 125, 0.6f),
                                new PointWithQualityImpl( 1477945624866L, 710, 0.8f),
                                new PointImpl( 1477985224866L, 7)
                        ),
                        Arrays.asList(
                                new PointImpl( 1477895624866L, 622),
                                new PointWithQualityImpl( 1477916224866L, -3, 0.4f),
                                new PointWithQualityImpl( 1477917224866L, 365, 0.7f),
                                new PointWithQualityImpl( 1477924624866L, 568, 0.6f),
                                new PointImpl( 1477948224866L, 14),
                                new PointWithQualityImpl( 1477957224866L, 86, 0.2f)
                        ),
                        Arrays.asList(
                                new PointWithQualityImpl( 1477895624866L, 622, 0.8f),
                                new PointWithQualityImpl( 1477916224866L, -3, 0.4f),
                                new PointWithQualityImpl( 1477917224866L, 365, 0.7f),
                                new PointWithQualityImpl( 1477924624866L, 568, 0.6f),
                                new PointWithQualityImpl( 1477948224866L, 14, 0.8f),
                                new PointWithQualityImpl( 1477957224866L, 86, 0.2f)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_1);
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.GRAFANA,
                        new JsonObject().put(HttpServerVerticle.VERSION, GrafanaApiVersion.HURENCE_DATASOURCE_PLUGIN.toString()));
        HttpWithHistorianSolrITHelper.deployCustomHttpAndHistorianVerticle(container, vertx, httpConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityAvgAndReturningAllPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test1/request.json",
                "/http/grafana/hurence/query/testWithQuality/test1/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityMinAndReturningAllPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test2/request.json",
                "/http/grafana/hurence/query/testWithQuality/test2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityMaxAndReturningAllPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test3/request.json",
                "/http/grafana/hurence/query/testWithQuality/test3/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithoutQualityAndReturningPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test4/request.json",
                "/http/grafana/hurence/query/testWithQuality/test4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityAvgAndReturningAllMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test5/request.json",
                "/http/grafana/hurence/query/testWithQuality/test5/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityMinAndReturningAllMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test6/request.json",
                "/http/grafana/hurence/query/testWithQuality/test6/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithQualityMaxAndReturningAllMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test7/request.json",
                "/http/grafana/hurence/query/testWithQuality/test7/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithoutQualityAndReturningMixedPoints(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/testWithQuality/test8/request.json",
                "/http/grafana/hurence/query/testWithQuality/test8/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
