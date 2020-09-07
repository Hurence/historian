package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.Version0SolrInjectorMultipleMetricSpecificPoints;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.logisland.record.Point;
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
import io.vertx.reactivex.ext.web.client.WebClient;
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
public class QueryEndPointVersion0IT extends AbstractQueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointVersion0IT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        initSolrAndVerticles(client, container, vertx, context);
        injectChunksIntoSolr(client, vertx);
    }

    private static String metricNameSpecialCharacters1 = "metric with spaces";
    private static String metricNameSpecialCharacters2 = "metric (with && special characters)";

    public static void injectChunksIntoSolr(SolrClient client, Vertx vertx) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new Version0SolrInjectorMultipleMetricSpecificPoints(
                Arrays.asList("temp_a", "temp_b", "maxDataPoints", metricNameSpecialCharacters1, metricNameSpecialCharacters2),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 1477895624866L, 622),
                                new Point(0, 1477916224866L, -3),
                                new Point(0, 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                new Point(0, 1477895624866L, 861),
                                new Point(0, 1477917224866L, 767)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new Point(0, 1477895624866L, 1),
                                new Point(0, 1477895624867L, 1),
                                new Point(0, 1477895624868L, 1),
                                new Point(0, 1477895624869L, 1),
                                new Point(0, 1477895624870L, 1),
                                new Point(0, 1477895624871L, 1),
                                new Point(0, 1477895624872L, 1),
                                new Point(0, 1477895624873L, 1),
                                new Point(0, 1477895624874L, 1),
                                new Point(0, 1477895624875L, 1),
                                new Point(0, 1477895624876L, 1),
                                new Point(0, 1477895624877L, 1),
                                new Point(0, 1477895624878L, 1),
                                new Point(0, 1477895624879L, 1),
                                new Point(0, 1477895624880L, 1),
                                new Point(0, 1477895624881L, 1),
                                new Point(0, 1477895624882L, 1),
                                new Point(0, 1477895624883L, 1),
                                new Point(0, 1477895624884L, 1),
                                new Point(0, 1477895624885L, 1),
                                new Point(0, 1477895624886L, 1),
                                new Point(0, 1477895624887L, 1),
                                new Point(0, 1477895624888L, 1),
                                new Point(0, 1477895624889L, 1),
                                new Point(0, 1477895624890L, 1),
                                new Point(0, 1477895624891L, 1),
                                new Point(0, 1477895624892L, 1),
                                new Point(0, 1477895624893L, 1),
                                new Point(0, 1477895624894L, 1),
                                new Point(0, 1477895624895L, 1),
                                new Point(0, 1477895624896L, 1),
                                new Point(0, 1477895624897L, 1),
                                new Point(0, 1477895624898L, 1),
                                new Point(0, 1477895624899L, 1),
                                new Point(0, 1477895624900L, 1),
                                new Point(0, 1477895624901L, 1),
                                new Point(0, 1477895624902L, 1),
                                new Point(0, 1477895624903L, 1),
                                new Point(0, 1477895624904L, 1),
                                new Point(0, 1477895624905L, 1)
                        ),
                        Arrays.asList(
                                new Point(0, 1477895624866L, 861),
                                new Point(0, 1477917224866L, 767)
                        ),
                        Arrays.asList(
                                new Point(0, 1477895624868L, 861),
                                new Point(0, 1477917224869L, 767)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

    public static void initSolrAndVerticles(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException, InterruptedException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.GRAFANA,
                        new JsonObject().put(HttpServerVerticle.VERSION, GrafanaApiVersion.HURENCE_DATASOURCE_PLUGIN.toString()));
        HttpWithHistorianSolrITHelper.deployCustomHttpAndHistorianVerticle(container, vertx, httpConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }
}
