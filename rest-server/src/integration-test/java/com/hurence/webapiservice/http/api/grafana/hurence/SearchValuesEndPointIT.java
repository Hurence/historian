package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.model.SchemaVersion;
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
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.OK;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class SearchValuesEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchValuesEndPointIT.class);
    private static WebClient webClient;

    @BeforeAll
    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolr(container);
        injectChunksIntoSolr(client);
        Completable verticlesDeployed = initVerticles(container, vertx, context);
        verticlesDeployed
                .subscribe(context::completeNow,
                        context::failNow);
    }

    public static void injectChunksIntoSolr(SolrClient client) throws IOException, SolrServerException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralInjectorCurrentVersion injector = new GeneralInjectorCurrentVersion();
        Chunk chunkTempbUsine1Sensor3 = ChunkBuilderHelper.fromPointsAndTags("temp_b", Arrays.asList(
                    Measure.fromValue(1, 1)
                ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_3");
                    put("usine", "usine_1");
                }}
        );
        injector.addChunk(chunkTempbUsine1Sensor3);

        Chunk chunkTempaUsine1Sensor1 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 2, 2),
                Measure.fromValue( 3, 3),
                Measure.fromValue( 4, 4)
        ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_1");
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1Sensor1);

        Chunk chunkTempaUsine1Sensor2 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 5, 5)
        ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_2");
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1Sensor2);

        Chunk chunkTempaUsine2Sensor3 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 6, 6)
        ),
                new HashMap<String, String>() {{
                    put("sensor", "sensor_3");
                    put("usine", "usine_2");
                }});
        injector.addChunk(chunkTempaUsine2Sensor3);

        Chunk chunkTempaUsine1 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 7, 7)
        ),
                new HashMap<String, String>() {{
                    put("usine", "usine_1");
                }});
        injector.addChunk(chunkTempaUsine1);

        Chunk chunkTempaUsine3 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 8, 8)
        ),
                new HashMap<String, String>() {{
                    put("usine", "usine_3");
                }});
        injector.addChunk(chunkTempaUsine3);

        Chunk chunkTempaNoUsine = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue( 9, 9)
        ),
                new HashMap<String, String>() {{
                    put("usine", "no_usine");
                }});
        injector.addChunk(chunkTempaNoUsine);

        Chunk chunkWithEconomyTag1 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(10, 10)
        ),
                new HashMap<String, String>() {{
                    put("Economy (GDP per Capita)", "1.44178");
                }});
        injector.addChunk(chunkWithEconomyTag1);

        Chunk chunkWithEconomyTag2 = ChunkBuilderHelper.fromPointsAndTags("temp_a", Arrays.asList(
                Measure.fromValue(11, 11)
        ),
                new HashMap<String, String>() {{
                    put("Economy (GDP per Capita)", "1.52733");
                }});
        injector.addChunk(chunkWithEconomyTag2);

        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    private static void initSolr(DockerComposeContainer container) throws InterruptedException, SolrServerException, IOException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        SolrITHelper.addFieldToChunkSchema(container, "usine");
        SolrITHelper.addFieldToChunkSchema(container, "sensor");
        SolrITHelper.addFieldToChunkSchema(container, "Economy (GDP per Capita)");
    }

    public static Completable initVerticles(DockerComposeContainer container, Vertx vertx, VertxTestContext context) {
        return HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx).ignoreElement();
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
    public void testSearchSensorsValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchSensorValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchSensorValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsinesValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchNamesValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchNameValues/request.json",
                        "/http/grafana/hurence/searchValues/testSearchNameValues/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsineValuesWithQuery(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithQuery/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithQuery/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchUsineValuesWithLimit(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithLimit/request.json",
                        "/http/grafana/hurence/searchValues/testSearchUsineValuesWithLimit/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /**
     * Bug issue #152  at https://github.com/Hurence/historian/issues/152
     * Solved the 3 September 2020
     *
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchEconomyValues(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchTagValuesWithSpaceAndParentheses/request.json",
                        "/http/grafana/hurence/searchValues/testSearchTagValuesWithSpaceAndParentheses/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    /**
     * Bug issue #152  at https://github.com/Hurence/historian/issues/152
     * Solved the 3 September 2020
     *
     * @param vertx
     * @param testContext
     */
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearchEconomyValues2(Vertx vertx, VertxTestContext testContext) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonArray>(HURENCE_DATASOURCE_GRAFANA_SEARCH_VALUES_API_ENDPOINT,
                        "/http/grafana/hurence/searchValues/testSearchTagValuesWithSpaceAndParentheses2/request.json",
                        "/http/grafana/hurence/searchValues/testSearchTagValuesWithSpaceAndParentheses2/expectedResponse.json",
                        OK, StatusMessages.OK,
                        BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

}
