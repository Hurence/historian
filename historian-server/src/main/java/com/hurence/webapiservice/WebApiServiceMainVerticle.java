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
  private static final String CONFIG_HTTP_SERVER_ROOT = "http_server";
  private static final String CONFIG_HISTORIAN_ROOT = "historian";
  private static final String CONFIG_INSTANCE_NUMBER_WEB = "web.verticles.instance.number";
  private static final String CONFIG_INSTANCE_NUMBER_HISTORIAN = "historian.verticles.instance.number";

  //conf default values
  private static final int DEFAULT_INSTANCE_NUMBER_WEB = 2;
  private static final int DEFAULT_INSTANCE_NUMBER_HISTORIAN = 1;

  private static final List<String> ACCEPTED_CONFS = Arrays.asList(CONFIG_INSTANCE_NUMBER_WEB,
          CONFIG_INSTANCE_NUMBER_HISTORIAN);

  @Override
  public void start(Promise<Void> promise) throws Exception {
    //set this property so vertx use slf4j logging.
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    vertx.getOrCreateContext();
    LOGGER.info("deploying {} verticle using config : {}\n The final config is {}",
            WebApiServiceMainVerticle.class.getSimpleName(),
            config().encodePrettily(),
            getFinalConfig());
    Single<String> dbVerticleDeployment = deployHistorianVerticle();
    dbVerticleDeployment
            .flatMap(id -> deployHttpVerticle())
            .doOnError(promise::fail)
            .doOnSuccess(id -> promise.complete())
            .subscribe(id -> {
              LOGGER.info("{} finished to deploy verticles", WebApiServiceMainVerticle.class.getSimpleName());
            });
  }

  private JsonObject getFinalConfig() {
    JsonObject config = config();
    removeUnknownConf(config);
    addDefaultsValues(config);
    checkConfIsCorrect(config);
    return config;
  }

  private void checkConfIsCorrect(JsonObject config) {

  }

  private void addDefaultsValues(JsonObject config) {
    if (!config.containsKey(CONFIG_INSTANCE_NUMBER_HISTORIAN)) {
      config.put(CONFIG_INSTANCE_NUMBER_HISTORIAN, DEFAULT_INSTANCE_NUMBER_HISTORIAN);
    }
    if (!config.containsKey(CONFIG_INSTANCE_NUMBER_WEB)) {
      config.put(CONFIG_INSTANCE_NUMBER_WEB, DEFAULT_INSTANCE_NUMBER_WEB);
    }
  }

  private void removeUnknownConf(JsonObject config) {
    config.fieldNames().forEach(f -> {
      if (!ACCEPTED_CONFS.contains(f)) {
        config.remove(f);
      }
    });
  }

  private Single<String> deployHistorianVerticle() {
    int instances = config().getInteger(CONFIG_INSTANCE_NUMBER_HISTORIAN);
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HISTORIAN_ROOT));
    return vertx.rxDeployVerticle(HistorianVerticle::new, opts);
  }

  private Single<String> deployHttpVerticle() {
    int instances = config().getInteger(CONFIG_INSTANCE_NUMBER_WEB);
    DeploymentOptions opts = new DeploymentOptions().setInstances(instances).setConfig(config().getJsonObject(CONFIG_HTTP_SERVER_ROOT));
    return vertx.rxDeployVerticle(HttpServerVerticle::new, opts);
  }

}
