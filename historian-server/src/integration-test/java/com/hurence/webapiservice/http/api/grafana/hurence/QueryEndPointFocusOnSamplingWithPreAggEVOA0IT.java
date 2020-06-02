package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralEVOA0SolrInjector;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.api.grafana.GrafanaApiVersion;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnSamplingWithPreAggEVOA0IT extends AbstractQueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnSamplingWithPreAggEVOA0IT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.EVOA0);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        JsonObject historianConf = buildHistorianConf();
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.GRAFANA,
                        new JsonObject().put(HttpServerVerticle.VERSION, GrafanaApiVersion.HURENCE_DATASOURCE_PLUGIN.toString()));
        HttpWithHistorianSolrITHelper
                .deployCustomHttpAndCustomHistorianVerticle(container, vertx, historianConf, httpConf)
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
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L)
                .put(HistorianVerticle.CONFIG_SCHEMA_VERSION, SchemaVersion.EVOA0.toString());
    }

    public static SolrInjector buildInjector() {
        List<List<Point>> pointsByChunk10Chunks = Arrays.asList(
                Arrays.asList(
                        new Point(0, 1L, 1.0),
                        new Point(0, 2L, 1.0)
                ),
                Arrays.asList(
                        new Point(0, 3L, 2.0),
                        new Point(0, 4L, 2.0)
                ),
                Arrays.asList(
                        new Point(0, 5L, 3.0),
                        new Point(0, 6L, 3.0)
                ),
                Arrays.asList(
                        new Point(0, 7L, 4.0),
                        new Point(0, 8L, 4.0)
                ),
                Arrays.asList(
                        new Point(0, 9L, 5.0),
                        new Point(0, 10L, 5.0)
                ),
                Arrays.asList(
                        new Point(0, 11L, 6.0),
                        new Point(0, 12L, 6.0)
                ),
                Arrays.asList(
                        new Point(0, 13L, 7.0),
                        new Point(0, 14L, 7.0)
                ),
                Arrays.asList(
                        new Point(0, 15L, 8.0),
                        new Point(0, 16L, 8.0)
                ),
                Arrays.asList(
                        new Point(0, 17L, 9.0),
                        new Point(0, 18L, 9.0)
                ),
                Arrays.asList(
                        new Point(0, 19L, 10.0),
                        new Point(0, 20L, 10.0)
                )
        );
        final GeneralEVOA0SolrInjector injector = new GeneralEVOA0SolrInjector();
        pointsByChunk10Chunks.forEach(pts -> {
            injector.addChunk("metric_10_chunk", "test", pts);
        });
        pointsByChunk10Chunks.stream().limit(9).collect(Collectors.toList()).forEach(pts -> {
            injector.addChunk("metric_9_chunk", "test", pts);
        });
        pointsByChunk10Chunks.stream().limit(7).collect(Collectors.toList()).forEach(pts -> {
            injector.addChunk("metric_7_chunk", "test", pts);
        });
        pointsByChunk10Chunks.stream().limit(5).collect(Collectors.toList()).forEach(pts -> {
            injector.addChunk("metric_5_chunk", "test", pts);
        });
        Arrays.asList(
                pointsByChunk10Chunks.stream().flatMap(List::stream).collect(Collectors.toList())
        ).forEach(pts -> {
            injector.addChunk("metric_1_chunk_of_20_points", "test", pts);
        });
        return injector;
    }
}
