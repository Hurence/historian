package com.hurence.webapiservice.http.ingestion;

import com.hurence.webapiservice.http.grafana.util.DateRequestParserUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.hurence.historian.modele.HistorianFields.METRIC_NAME_REQUEST_FIELD;
import static com.hurence.historian.modele.HistorianFields.POINTS_REQUEST_FIELD;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);


    public ResponseAndErrorHolder checkAndBuildValidHistorianImportRequest(JsonArray metricsParam) throws IllegalArgumentException {
        // TODO should i keep this ?
        /*if (null == metricsParam) {
            throw new IllegalArgumentException("parameter array is NULL");
        }else if (metricsParam.isEmpty()) {
            throw new IllegalArgumentException("parameter array is empty");
        }*/

        ResponseAndErrorHolder responseAndErrorHolder = new ResponseAndErrorHolder();
        for (Object object : metricsParam) {
            JsonObject timeserie = (JsonObject) object;
            if ((timeserie.getString(METRIC_NAME_REQUEST_FIELD) == null) || (timeserie.getJsonArray(POINTS_REQUEST_FIELD) == null)) {
                responseAndErrorHolder.errorMessages.add("can't add this object " + timeserie.toString());
                continue;
            }
            JsonObject newTimeserie = new JsonObject();
            newTimeserie.put(METRIC_NAME_REQUEST_FIELD, timeserie.getString(METRIC_NAME_REQUEST_FIELD));
            JsonArray newPoints = new JsonArray();
            for (Object point : timeserie.getJsonArray(POINTS_REQUEST_FIELD)) {
                JsonArray pointArray = (JsonArray) point;
                if ((pointArray.size() != 2) || (pointArray.getLong(0) == null) || (pointArray.getDouble(1) == null)) {
                    responseAndErrorHolder.errorMessages.add("can't add this point " + pointArray.toString() + " of this object " + timeserie.toString());
                    continue;
                } else
                    newPoints.add(pointArray);
            }
            newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
            responseAndErrorHolder.correctPoints.add(newTimeserie);
        }
        if (responseAndErrorHolder.correctPoints.isEmpty())
            throw new IllegalArgumentException("all points are invalid");
        return responseAndErrorHolder;

    }

    public static class ResponseAndErrorHolder {

        public JsonArray correctPoints;
        public ArrayList<String> errorMessages;

        ResponseAndErrorHolder () {
            this.correctPoints = new JsonArray();
            this.errorMessages = new ArrayList<>();
        }
    }
}
