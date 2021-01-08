package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HttpITHelper;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public abstract class AbstractQueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractQueryEndPointFocusOnSamplingWithPreAgg.class);
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        assertHelper = new AssertResponseGivenRequestHelper(HttpITHelper.buildWebClient(vertx), HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT);
    }

    @AfterAll
    public static void closeVertx(Vertx vertx, VertxTestContext context) {
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax20(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric10ChunkMaxPoint20/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric10ChunkMaxPoint20/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric10ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric10ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric9ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric9ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric9ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric7ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric7ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric7ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric5ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric5ChunkMaxPoint4/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric5ChunkMaxPoint4/expectedResponse.json");
    }

    //@TODO fix that one day
    //@Test
    //@Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric1ChunkOf20PointMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric1ChunkOf20PointMaxPoint4/request.json",
                "/http/grafana/hurence/query/preagg-sampling-algo/testMetric1ChunkOf20PointMaxPoint4/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
