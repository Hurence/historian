package com.hurence.webapiservice.http.ingestion;

import com.hurence.webapiservice.http.grafana.QueryRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.historian.modele.HistorianFields.METRIC_NAME_REQUEST_FIELD;
import static com.hurence.historian.modele.HistorianFields.POINTS_REQUEST_FIELD;
import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);


    static void checkRequest (JsonArray getMetricsParam) {
        try {
            for (Object object : getMetricsParam) {
                JsonObject jsonObject = (JsonObject) object;
                final boolean[] boo3 = new boolean[1];
                boo3[0] = true;
                jsonObject.getJsonArray(POINTS_REQUEST_FIELD).forEach(point -> {
                    JsonArray pointArray = (JsonArray) point;
                    boo3[0] =  boo3[0] && pointArray.size() == 2;
                });
                if (!jsonObject.containsKey(METRIC_NAME_REQUEST_FIELD) && !jsonObject.containsKey(POINTS_REQUEST_FIELD) && boo3[0]) {
                    throw new IllegalArgumentException("bad request");
                }
            }
        }catch (Exception ex) {
        LOGGER.error("can't parsing request", ex);
        }
    }
}
