package com.hurence.webapiservice;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.webapiservice.WebApiServiceMainVerticle.*;

public class WebApiMainVerticleConf {
    private static Logger LOGGER = LoggerFactory.getLogger(WebApiMainVerticleConf.class);
    private final int numberOfHistorianVerticles;
    private final int numberOfHttpVerticles;
    private final String tagsLookupfilePath;
    private final char tagsLookupSeparator;
    private final boolean tagsLookupEnabled;

    public WebApiMainVerticleConf(JsonObject json) {
        LOGGER.info("loading conf from json : {}", json.encode());
        this.numberOfHistorianVerticles = json.getInteger(CONFIG_INSTANCE_NUMBER_HISTORIAN, 1);
        this.numberOfHttpVerticles = json.getInteger(CONFIG_INSTANCE_NUMBER_WEB, 1);
        this.tagsLookupfilePath = json.getString(CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_CSV_FILE_PATH, "");
        this.tagsLookupSeparator = json.getString(CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_CSV_SEPARATOR, ",").charAt(0);
        this.tagsLookupEnabled = json.getBoolean(CONFIG_HISTORIAN_METRIC_NAME_LOOKUP_ENABLED, false);
    }

    public int getNumberOfInstanceHistorian() {
        return numberOfHistorianVerticles;
    }

    public int getNumberOfInstanceHttpVerticle() {
        return numberOfHttpVerticles;
    }

    public String getTagsLookupfilePath() {
        return tagsLookupfilePath;
    }

    public char getTagsLookupSeparator() {
        return tagsLookupSeparator;
    }

    public boolean isTagsLookupEnabled() {
        return tagsLookupEnabled;
    }


    @Override
    public String toString() {
        return "WebApiMainVerticleConf{" +
                "numberOfHistorianVerticles=" + numberOfHistorianVerticles +
                ", numberOfHttpVerticles=" + numberOfHttpVerticles +
                ", tagsLookupfilePath='" + tagsLookupfilePath + '\'' +
                ", tagsLookupSeparator=" + tagsLookupSeparator +
                ", tagsLookupEnabled=" + tagsLookupEnabled +
                '}';
    }
}
