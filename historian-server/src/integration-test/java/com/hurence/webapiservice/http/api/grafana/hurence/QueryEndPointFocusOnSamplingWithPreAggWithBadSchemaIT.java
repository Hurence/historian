package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.api.grafana.GrafanaApiVersion;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.DeploymentOptions;
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

import static com.hurence.webapiservice.util.HistorianSolrITHelper.HISTORIAN_ADRESS;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnSamplingWithPreAggWithBadSchemaIT extends QueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnSamplingWithPreAggWithBadSchemaIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.EVOA0);
        injectChunksIntoSolr(client, vertx);
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, HttpITHelper.PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, HttpITHelper.HOST)
                .put(HttpServerVerticle.GRAFANA,
                        new JsonObject().put(HttpServerVerticle.VERSION, GrafanaApiVersion.HURENCE_DATASOURCE_PLUGIN.toString()));
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(httpConf);
        JsonObject historianConf = buildHistorianConf();
        historianConf.put(HistorianVerticle.CONFIG_SCHEMA_VERSION, SchemaVersion.EVOA0.toString());
        HistorianSolrITHelper.deployHistorianVerticle(container, vertx, historianConf)
                .flatMap(id -> vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions))
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                }).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

}
