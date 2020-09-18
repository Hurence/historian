package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.reactivex.Completable;
import io.reactivex.Single;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointShouldTruncateChunksIfNeededCurrentVersionIT {

    private static WebClient webClient;
    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointShouldTruncateChunksIfNeededCurrentVersionIT.class);

    @BeforeAll
    public static void setupClient(Vertx vertx) {
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void closeWebClient() {
        webClient.close();
    }

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        context.completeNow();
    }

    @AfterEach
    public void undeployVerticles(Vertx vertx, VertxTestContext context) {
        List<Completable> undeployements = vertx.deploymentIDs().stream()
                .map(vertx::rxUndeploy)
                .collect(Collectors.toList());
        Completable all = Completable.merge(undeployements);
        all.subscribe(
                () -> { context.completeNow(); },
                e -> { context.failNow(e); }
        );
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


    public static Single<String> initVerticlesWithQueryMode1(DockerComposeContainer container, Vertx vertx)  {
        return HttpWithHistorianSolrITHelper
                .deployHttpAndHistorianVerticle(container, vertx);
    }

    public static Single<String> initVerticlesWithQueryMode2(DockerComposeContainer container, Vertx vertx)  {
        JsonObject historianConf = buildHistorianConf();
        return HttpWithHistorianSolrITHelper
                .deployHttpAndCustomHistorianVerticle(container, vertx, historianConf);
    }

    public static JsonObject buildHistorianConf() {
        return new JsonObject()
                    //10 so if more than 5 chunk (of size 2) returned we should sample
                    //with pre aggs
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 0L)
                    .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 0L);
    }

    public static SolrInjector buildInjector() {
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
     * If we get a chunk with chunk_start : t1 and chunk_end: t2.
     * Then if we do a request from "tfrom" to "tto".
     * And if the following is true :  t1 < tfrom < ttp < t2
     * Then we should not get point outside of scope [tfrom, tto]
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testExpect2PointsQueryMode1(DockerComposeContainer container,
                                            Vertx vertx,
                                            VertxTestContext testContext) {
        initVerticlesWithQueryMode1(container, vertx)
                .doOnError(testContext::failNow)
                .blockingGet();
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test1QueryMode1/request.json",
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test1QueryMode1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /**
     * If we get a chunk with chunk_start : t1 and chunk_end: t2.
     * Then if we do a request from "tfrom" to "tto".
     * And if the following is true :  t1 < tfrom < ttp < t2
     * Then we should not get point outside of scope [tfrom, tto]
     * And this should be the case for every chunks if there is several chunks !
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testExpect6PointsQueryMode1(DockerComposeContainer container,
                                            Vertx vertx,
                                            VertxTestContext testContext) {
        initVerticlesWithQueryMode1(container, vertx)
                .doOnError(testContext::failNow)
                .blockingGet();
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test2QueryMode1/request.json",
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test2QueryMode1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }


    /**
     * If we get a chunk with chunk_start : t1 and chunk_end: t2.
     * Then if we do a request from "tfrom" to "tto".
     * And if the following is true :  t1 < tfrom < ttp < t2
     * Then we should not get point outside of scope [tfrom, tto]
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testExpect1PointQueryMode2(DockerComposeContainer container,
                                           Vertx vertx,
                                           VertxTestContext testContext) {
        initVerticlesWithQueryMode2(container, vertx)
                .doOnError(testContext::failNow)
                .blockingGet();
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test1QueryMode2/request.json",
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test1QueryMode2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /**
     * If we get a chunk with chunk_start : t1 and chunk_end: t2.
     * Then if we do a request from "tfrom" to "tto".
     * And if the following is true :  t1 < tfrom < ttp < t2
     * Then we should not get point outside of scope [tfrom, tto]
     * And this should be the case for every chunks if there is several chunks !
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testExpect2PointQueryMode2(DockerComposeContainer container,
                                           Vertx vertx,
                                           VertxTestContext testContext) {
        initVerticlesWithQueryMode2(container, vertx)
                .doOnError(testContext::failNow)
                .blockingGet();
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test2QueryMode2/request.json",
                        "/http/grafana/hurence/query/shouldTruncateChunksIfNeeded/test2QueryMode2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}
