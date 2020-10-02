package com.hurence.webapiservice;

import io.vertx.core.json.JsonObject;

import static com.hurence.webapiservice.WebApiServiceMainVerticle.CONFIG_INSTANCE_NUMBER_HISTORIAN;
import static com.hurence.webapiservice.WebApiServiceMainVerticle.CONFIG_INSTANCE_NUMBER_WEB;

public class WebApiMainVerticleConf {

    private final int numberOfHistorianVerticles;
    private final int numberOfHttpVerticles;

    public WebApiMainVerticleConf(JsonObject json) {
        this.numberOfHistorianVerticles = json.getInteger(CONFIG_INSTANCE_NUMBER_HISTORIAN, 1);
        this.numberOfHttpVerticles = json.getInteger(CONFIG_INSTANCE_NUMBER_WEB, 1);
    }

    public int getNumberOfInstanceHistorian() {
        return numberOfHistorianVerticles;
    }

    public int getNumberOfInstanceHttpVerticle() {
        return numberOfHttpVerticles;
    }

    @Override
    public String toString() {
        return "WebApiMainVerticleConf{" +
                "numberOfHistorianVerticles=" + numberOfHistorianVerticles +
                ", numberOfHttpVerticles=" + numberOfHttpVerticles +
                '}';
    }
}
