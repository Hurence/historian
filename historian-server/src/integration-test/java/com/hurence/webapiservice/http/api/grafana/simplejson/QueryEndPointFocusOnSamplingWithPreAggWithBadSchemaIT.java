package com.hurence.webapiservice.http.api.grafana.simplejson;

import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.historian.compatibility.SchemaVersion;
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

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnSamplingWithPreAggWithBadSchemaIT extends QueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryEndPointFocusOnSamplingWithPreAggWithBadSchemaIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createHistorianCollection(client, SolrExtension.SOLR_CONF_TEMPLATE_HISTORIAN_VERSION_0);
        injectChunksIntoSolr(client, vertx);
        JsonObject historianConf = buildHistorianConf();
        historianConf.put(HistorianVerticle.CONFIG_SCHEMA_VERSION, SchemaVersion.VERSION_0.toString());
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

}
