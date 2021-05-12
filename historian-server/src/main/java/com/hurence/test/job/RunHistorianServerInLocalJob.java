package com.hurence.test.job;

import com.hurence.webapiservice.WebApiServiceMainVerticle;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunHistorianServerInLocalJob {

    private static Logger LOGGER = LoggerFactory.getLogger(RunHistorianServerInLocalJob.class);
    private static Vertx vertx;

    public static void main(String[] args) {
        vertx = Vertx.vertx();
        deployWebApiServiceMainVerticle().subscribe();
        try {
            while (true) {
                LOGGER.info("sleep 100000");
                Thread.sleep(100000);
            }
        } catch (Exception ex) {
            LOGGER.error("Interrupted ?");
        }
    }

    public static Single<String> deployWebApiServiceMainVerticle() {
        DeploymentOptions conf = getDeploymentOptions();
        return vertx.rxDeployVerticle(new WebApiServiceMainVerticle(), conf)
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static DeploymentOptions getDeploymentOptions() {
        JsonObject historianConf = geHistorianVerticleConf();
        JsonObject httpConf = getHttpVerticleConf();
        return new DeploymentOptions().setConfig(
                new JsonObject()
                        .put(WebApiServiceMainVerticle.CONFIG_HTTP_SERVER_ROOT, httpConf)
                        .put(WebApiServiceMainVerticle.CONFIG_HISTORIAN_ROOT, historianConf)
                        .put("historian.metric_name_lookup.csv_file.path", "/Users/tom/Documents/workspace/ifpen/data-historian/conf/synonyms.csv")
                        .put("historian.metric_name_lookup.csv_file.separator", ";")
                        .put("historian.metric_name_lookup.enabled", true)
        );
    }

    private static JsonObject geHistorianVerticleConf() {
        JsonObject solrConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_SOLR_CHUNK_COLLECTION, "historian")
                .put(HistorianVerticle.CONFIG_SOLR_ANNOTATION_COLLECTION, "annotations")
                .put(HistorianVerticle.CONFIG_SOLR_USE_ZOOKEEPER, true)
                .put(HistorianVerticle.CONFIG_SOLR_ZOOKEEPER_URLS, new JsonArray().add("localhost:9983"))
                .put(HistorianVerticle.CONFIG_SOLR_STREAM_ENDPOINT, "http://localhost:8983/solr/historian");
//                .put(HistorianVerticle.MAX_NUMBER_OF_TARGET_RETURNED, 100);
//        JsonObject grafana = new JsonObject()
//                .put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject()
//                        .put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject()
//                                .put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 100)));
        return new JsonObject()
                .put(HistorianVerticle.CONFIG_ROOT_SOLR, solrConf);
//                .put(HistorianVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
//                .put(HistorianVerticle.CONFIG_API_HISTORAIN, grafana);
    }

    public static JsonObject getHttpVerticleConf() {
        return new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, 8081)
//                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, "0.0.0.0")
                .put(HttpServerVerticle.CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED, 10000)
                .put(HttpServerVerticle.CONFIG_DEBUG_MODE, true);
    }

}
