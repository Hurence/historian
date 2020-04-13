package com.hurence.webapiservice.http.ingestion;

import com.hurence.webapiservice.http.grafana.util.DateRequestParserUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.historian.modele.HistorianFields.METRIC_NAME_REQUEST_FIELD;
import static com.hurence.historian.modele.HistorianFields.POINTS_REQUEST_FIELD;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);


    public String[] checkRequest (JsonArray metricsParam) {
        final String[] error = new String[1];
        try {
            if (null == metricsParam) {
                error[0] = "parameter array is NULL";
                throw new IllegalArgumentException("parameter array is NULL");
            }else if (metricsParam.isEmpty()) {
                error[0] = "parameter array is empty";
                throw new IllegalArgumentException("parameter array is empty");
            }
            for (Object object : metricsParam) {
                JsonObject timeserie = (JsonObject) object;
                final boolean[] boo = new boolean[1];
                boo[0] = true;
                if (!timeserie.containsKey(METRIC_NAME_REQUEST_FIELD)) {
                    error[0] = "field \"name\" does not exist !";
                    throw new IllegalArgumentException("field \"name\" does not exist !");
                }else if (! (timeserie.getValue(METRIC_NAME_REQUEST_FIELD) instanceof String)){
                    error[0] = "metric name field exist, but should be a string";
                    throw new IllegalArgumentException("metric name field exist, but should be a string");
                }
                if (!timeserie.containsKey(POINTS_REQUEST_FIELD)) {
                    error[0] = "field \"points\" does not exist !";
                    throw new IllegalArgumentException("field \"points\" does not exist !");
                }else if (!(timeserie.getValue(POINTS_REQUEST_FIELD) instanceof JsonArray)){
                    error[0] = "points field exist, but should be of type JsonArray";
                    throw new IllegalArgumentException("points field exist, but should be of type JsonArray");
                }
                if (timeserie.containsKey(POINTS_REQUEST_FIELD)) {
                    timeserie.getJsonArray(POINTS_REQUEST_FIELD).forEach(point -> {
                        JsonArray pointArray = (JsonArray) point;
                        boo[0] = boo[0] && pointArray.size() == 2;
                        if (! boo[0]) {
                            error[0] = "invalid points size " + pointArray.toString();
                            throw new IllegalArgumentException("invalid points size " + pointArray.toString());
                        }else  if (!(pointArray.getValue(0) instanceof Long) && !(pointArray.getValue(0) instanceof Integer)) {
                            error[0] = String.format("this date value %s is not a long !", pointArray.getValue(0));
                            throw new IllegalArgumentException(
                                    String.format("this date value %s is not a long !", pointArray.getValue(0)));
                        }else if(!DateRequestParserUtil.checkValidDate(pointArray.getLong(0))) {
                            error[0] = String.format("this date  value %s could not be parsed as a valid date !",
                                    pointArray.getLong(0));
                            throw new IllegalArgumentException(
                                    String.format("this date  value %s could not be parsed as a valid date !",
                                            pointArray.getLong(0)));
                        }else if (!(pointArray.getValue(1) instanceof Double)) {
                            error[0] = "invalid Value : not a double " + pointArray.getValue(1);
                            throw new IllegalArgumentException("invalid Value : not a double " + pointArray.getValue(1));
                        }
                    });
                }
            }
        }catch (IllegalArgumentException ex) {
        LOGGER.error("can't parsing request", ex);
        }
        return error;
    }
}
