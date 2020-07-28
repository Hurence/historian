package com.hurence.webapiservice;

import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.http.HttpVerticleConf;
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
  private static final String CONFIG_HTTP_SERVER_ROOT = "http_server";
  private static final String CONFIG_HISTORIAN_ROOT = "historian";
  public static final String CONFIG_INSTANCE_NUMBER_WEB = "web.verticles.instance.number";
  public static final String CONFIG_INSTANCE_NUMBER_HISTORIAN = "historian.verticles.instance.number";

  //conf default values
  private static final int DEFAULT_INSTANCE_NUMBER_WEB = 2;
  private static final int DEFAULT_INSTANCE_NUMBER_HISTORIAN = 1;

  private static final List<String> ACCEPTED_CONFS = Arrays.asList(CONFIG_INSTANCE_NUMBER_WEB,
          CONFIG_INSTANCE_NUMBER_HISTORIAN);

  private WebApiMainVerticleConf conf;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    //set this property so vertx use slf4j logging.
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    vertx.getOrCreateContext();
    this.conf = parseConfig(config());
    LOGGER.info("deploying {} verticle with config : {}", WebApiServiceMainVerticle.class.getSimpleName(), this.conf);
    Single<String> dbVerticleDeployment = deployHistorianVerticle();
    dbVerticleDeployment
            .flatMap(id -> deployHttpVerticle())
            .doOnError(promise::fail)
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
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(extractHistorianConf(config()));
    return vertx.rxDeployVerticle(HistorianVerticle::new, opts);
  }

  public static JsonObject extractHistorianConf(JsonObject rootConf) {
    return rootConf.getJsonObject(CONFIG_HISTORIAN_ROOT);
  }

  public static JsonObject extractHttpConf(JsonObject rootConf) {
    return rootConf.getJsonObject(CONFIG_HTTP_SERVER_ROOT);
  }

  private Single<String> deployHttpVerticle() {
    int instances = this.conf.getNumberOfInstanceHttpVerticle();
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(extractHttpConf(config()));
    return vertx.rxDeployVerticle(HttpServerVerticle::new, opts);
  }

}
