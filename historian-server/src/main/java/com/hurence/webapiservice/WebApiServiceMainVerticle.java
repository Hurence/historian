package com.hurence.webapiservice;

import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;

import java.util.Arrays;
import java.util.List;

public class WebApiServiceMainVerticle extends AbstractVerticle {

    private static Logger LOGGER = LoggerFactory.getLogger(WebApiServiceMainVerticle.class);

    //conf fields
    public static final String CONFIG_HTTP_SERVER_ROOT = "http_server";
    public static final String CONFIG_HISTORIAN_ROOT = "historian";
    public static final String CONFIG_INSTANCE_NUMBER_WEB = "web.verticles.instance.number";
    public static final String CONFIG_INSTANCE_NUMBER_HISTORIAN = "historian.verticles.instance.number";
    public static final String CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_CSV_FILE_PATH = "historian.metric_name_lookup.csv_file.path";
    public static final String CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_CSV_SEPARATOR = "historian.metric_name_lookup.csv_file.separator";
    public static final String CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_ENABLED = "historian.metric_name_lookup.enabled";

    //conf default values
    private static final int DEFAULT_INSTANCE_NUMBER_WEB = 2;
    private static final int DEFAULT_INSTANCE_NUMBER_HISTORIAN = 1;

    private static final List<String> ACCEPTED_CONFS = Arrays.asList(CONFIG_INSTANCE_NUMBER_WEB,
            CONFIG_INSTANCE_NUMBER_HISTORIAN);

    private WebApiMainVerticleConf conf;

    @Override
    public void start(Promise<Void> promise) throws Exception {
        // load cache
        if (conf.isTagsLookupEnabled()){
            NameLookup.enable();
            NameLookup.loadCacheFromFile(conf.getTagsLookupfilePath(), conf.getTagsLookupSeparator());
        }


        // set this property so vertx use slf4j logging.
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        vertx.getOrCreateContext();
        this.conf = parseConfig(config());
        LOGGER.info("deploying {} verticle with config : {}", WebApiServiceMainVerticle.class.getSimpleName(), this.conf);
        LOGGER.debug("You see log level DEBUG");
        LOGGER.trace("You see log level TRACE");
        Single<String> dbVerticleDeployment = deployHistorianVerticle();
        dbVerticleDeployment
                .flatMap(id -> deployHttpVerticle())
                .doOnError(t -> {
                    LOGGER.error("Could not deploy historian !", t);
                    promise.fail(t);
                })
                .doOnSuccess(id -> promise.complete())
                .subscribe(id -> {
                    LOGGER.info("{} finished to deploy verticles", WebApiServiceMainVerticle.class.getSimpleName());
                });
    }

    private WebApiMainVerticleConf parseConfig(JsonObject config) {
        return new WebApiMainVerticleConf(config);
    }

    private Single<String> deployHistorianVerticle() {
        int instances = this.conf.getNumberOfInstanceHistorian();
        DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HISTORIAN_ROOT));
        return vertx.rxDeployVerticle(HistorianVerticle::new, opts);
    }

    private Single<String> deployHttpVerticle() {
        int instances = this.conf.getNumberOfInstanceHttpVerticle();
        DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HTTP_SERVER_ROOT));
        return vertx.rxDeployVerticle(HttpServerVerticle::new, opts);
    }

}
