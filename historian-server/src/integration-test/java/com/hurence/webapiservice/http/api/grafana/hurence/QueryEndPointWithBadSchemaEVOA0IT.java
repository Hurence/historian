package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralEVOA0SolrInjector;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.timeseries.modele.PointImpl;
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

//import io.vertx.ext.web.client.WebClient;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointWithBadSchemaEVOA0IT extends AbstractQueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointWithBadSchemaEVOA0IT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.EVOA0);
        injectChunksIntoSolr(client);
        JsonObject historianConf = new JsonObject();
        historianConf.put(HistorianVerticle.CONFIG_SCHEMA_VERSION, SchemaVersion.EVOA0.toString());
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
    private static String metricNameSpecialCharacters2 = "metric (with && special characters)";

    public static void injectChunksIntoSolr(SolrClient client) throws SolrServerException, IOException {
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralEVOA0SolrInjector injector = new GeneralEVOA0SolrInjector();
        injector.addChunk("temp_a", "test",
                Arrays.asList(
                        new PointImpl( 1477895624866L, 622),
                        new PointImpl( 1477916224866L, -3),
                        new PointImpl( 1477917224866L, 365)
                ));
        injector.addChunk("temp_b", "test",
                Arrays.asList(
                        new PointImpl( 1477895624866L, 861),
                        new PointImpl( 1477917224866L, 767)
                ));
        injector.addChunk("maxDataPoints", "test",
                Arrays.asList(//maxDataPoints we are not testing value only sampling
                        new PointImpl( 1477895624866L, 1),
                        new PointImpl( 1477895624867L, 1),
                        new PointImpl( 1477895624868L, 1),
                        new PointImpl( 1477895624869L, 1),
                        new PointImpl( 1477895624870L, 1),
                        new PointImpl( 1477895624871L, 1),
                        new PointImpl( 1477895624872L, 1),
                        new PointImpl( 1477895624873L, 1),
                        new PointImpl( 1477895624874L, 1),
                        new PointImpl( 1477895624875L, 1),
                        new PointImpl( 1477895624876L, 1),
                        new PointImpl( 1477895624877L, 1),
                        new PointImpl( 1477895624878L, 1),
                        new PointImpl( 1477895624879L, 1),
                        new PointImpl( 1477895624880L, 1),
                        new PointImpl( 1477895624881L, 1),
                        new PointImpl( 1477895624882L, 1),
                        new PointImpl( 1477895624883L, 1),
                        new PointImpl( 1477895624884L, 1),
                        new PointImpl( 1477895624885L, 1),
                        new PointImpl( 1477895624886L, 1),
                        new PointImpl( 1477895624887L, 1),
                        new PointImpl( 1477895624888L, 1),
                        new PointImpl( 1477895624889L, 1),
                        new PointImpl( 1477895624890L, 1),
                        new PointImpl( 1477895624891L, 1),
                        new PointImpl( 1477895624892L, 1),
                        new PointImpl( 1477895624893L, 1),
                        new PointImpl( 1477895624894L, 1),
                        new PointImpl( 1477895624895L, 1),
                        new PointImpl( 1477895624896L, 1),
                        new PointImpl( 1477895624897L, 1),
                        new PointImpl( 1477895624898L, 1),
                        new PointImpl( 1477895624899L, 1),
                        new PointImpl( 1477895624900L, 1),
                        new PointImpl( 1477895624901L, 1),
                        new PointImpl( 1477895624902L, 1),
                        new PointImpl( 1477895624903L, 1),
                        new PointImpl( 1477895624904L, 1),
                        new PointImpl( 1477895624905L, 1)
                ));
        injector.addChunk("metric with spaces", "test",
                Arrays.asList(
                        new PointImpl( 1477895624866L, 861),
                        new PointImpl( 1477917224866L, 767)
                ));
        injector.addChunk(metricNameSpecialCharacters2, "test",
                Arrays.asList(
                        new Point(0, 1477895624868L, 861),
                        new Point(0, 1477917224869L, 767)
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }

}
