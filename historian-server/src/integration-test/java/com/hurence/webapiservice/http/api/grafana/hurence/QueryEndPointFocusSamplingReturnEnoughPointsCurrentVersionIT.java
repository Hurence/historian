package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.jetbrains.annotations.NotNull;
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
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusSamplingReturnEnoughPointsCurrentVersionIT {

    private static WebClient webClient;
    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusSamplingReturnEnoughPointsCurrentVersionIT.class);

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void closeVertx(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        initVerticlesThenFinish(container, vertx, context);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }


    public static void initVerticlesThenFinish(DockerComposeContainer container, Vertx vertx, VertxTestContext context)  {
        JsonObject historianConf = buildHistorianConf();
        HttpWithHistorianSolrITHelper
                .deployHttpAndCustomHistorianVerticle(container, vertx, historianConf)
                .subscribe(id -> {
                            context.completeNow();
                        },
                        t -> context.failNow(t));
    }

    public static JsonObject buildHistorianConf() {
        JsonObject historianConf = new JsonObject()
                //10 so if more than 5 chunk (of size 2) returned we should sample
                //with pre aggs
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 10L)
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L);
        return historianConf;
    }

    public static SolrInjector buildInjector() {
        List<List<Point>> pointsByChunk10Chunks = createChunks(1, 80000, 55);
        SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion injector = new SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(
                "metric_10_chunk", pointsByChunk10Chunks
        );
        return injector;

    }

    @NotNull
    private static List<List<Point>> createChunks(long firstPointTtimestamp,
                                                      long lastPointTtimestamp,
                                                      int numberOfChunk) {
        long numberOfTotalPoint = lastPointTtimestamp - firstPointTtimestamp;
        long numberOfPointByChunk = numberOfTotalPoint / (long)numberOfChunk;
        return IntStream.range(1, numberOfChunk)
            .mapToObj(chunkIndex -> {
                long fromChunk = firstPointTtimestamp + ((chunkIndex-1) * numberOfPointByChunk);
                long toChunk = chunkIndex * numberOfPointByChunk;//TODO test algo ok
                return createPointFromTo(fromChunk, toChunk);
            })
            .collect(Collectors.toList());
    }

    private static List<Point> createPointFromTo(long from, long to) {
        return LongStream.range(from, to)
                .mapToObj(l -> {
                    return Point.fromValueAndQuality(l, l, 1f);
                })
                .collect(Collectors.toList());
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test1(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test1/request.json",
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test2(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test2/request.json",
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void test3(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test3/request.json",
                        "/http/grafana/hurence/query/samplingShouldAlwaysReturnEnoughPoint/test3/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}
