package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.reactivex.Completable;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianVerticle.CONFIG_SCHEMA_VERSION;
import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ComplexQueryEndPointCurrentVersionIT {

    private static Logger LOGGER = LoggerFactory.getLogger(ComplexQueryEndPointCurrentVersionIT.class);
    private static WebClient webClient;

    @BeforeAll
    public static void initSolrAnderticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        Completable solrAndVerticlesDeployed = initSolrAndVerticles(container, vertx);
        Completable injectIntoSolr = Completable.fromCallable(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                injectChunksIntoSolr(client);
                return null;
            }
        });
        solrAndVerticlesDeployed
                .andThen(injectIntoSolr)
                .subscribe(context::completeNow,
                        context::failNow);


    }

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralInjectorCurrentVersion injector = new GeneralInjectorCurrentVersion();
        Chunk chunkTempbUsine1Sensor3 = ChunkBuilderHelper.fromPointsAndTags(
                "temp_b", Arrays.asList(Measure.fromValue(1, 1)),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_3");
                    put("usine", "usine_1");
                }}
        );
        injector.addChunk(chunkTempbUsine1Sensor3);

        Chunk chunkTempaUsine1Sensor1 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(2, 2),
                Measure.fromValue(3, 3),
                Measure.fromValue(4, 4)
                ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_1");
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1Sensor1);

        Chunk chunkTempaUsine1Sensor2 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(5, 3)
                ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_2");
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1Sensor2);

        Chunk chunkTempaUsine2Sensor3 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(6, 4)
                ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_3");
                    put("usine", "usine_2");
                }});
        injector.addChunk(chunkTempaUsine2Sensor3);

        Chunk chunkTempaUsine1 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(7, 5)
                ),
                new HashMap<String, String>() {{
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1);

        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static Completable initSolrAndVerticles(DockerComposeContainer container, Vertx vertx) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        SolrITHelper.addFieldToChunkSchema(container, "usine");
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
        JsonObject historianConf = new JsonObject()
                .put(CONFIG_SCHEMA_VERSION,
                        SchemaVersion.VERSION_0.toString());
        return HttpWithHistorianSolrITHelper.deployHttpAndCustomHistorianVerticle(container, vertx, historianConf).ignoreElement();
    }

    @BeforeAll
    public static void initWebclient(Vertx vertx) {
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void closeWebClientAndVerticles(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test1/request.json",
                        "/http/grafana/hurence/query/complex/test1/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithNoTags(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test2/request.json",
                        "/http/grafana/hurence/query/complex/test2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithAggregations(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test3/request.json",
                        "/http/grafana/hurence/query/complex/test3/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithDifferentTags(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test4/request.json",
                        "/http/grafana/hurence/query/complex/test4/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithRefIds(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_QUERY_API_ENDPOINT,
                        "/http/grafana/hurence/query/complex/test5/request.json",
                        "/http/grafana/hurence/query/complex/test5/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}