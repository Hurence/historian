package com.hurence.webapiservice.http.api.grafana.util;

import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.grafana.model.HurenceDatasourcePluginQueryRequestParam.DEFAULT_ALL_AGGREGATION_LIST;
import static com.hurence.webapiservice.modele.AGG.values;

public class RequestParserUtil {

    private RequestParserUtil() {};

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);
    //DOES NOT MAKE this variable global because it can be used in a multi threaded way !
    //private static SimpleDateFormat dateFormat = createDateFormat();

    public static SimpleDateFormat createDateFormat() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimeZone myTimeZone = TimeZone.getTimeZone(ZoneId.of("UTC"));
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

    public static Long parseDate(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (fromObj instanceof String) {
            String fromStr = (String) fromObj;
            try {
                //Have to create a local SimpleDAteFormat because this method can be used in a multi threaded way !
                return createDateFormat().parse(fromStr).getTime();
            } catch (ParseException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be parsed as a valid date !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a string !",
                        pointer, fromObj));
    }

    public static Map<String,String> parseMapStringString(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return Collections.emptyMap();
        if (fromObj instanceof JsonObject) {
            try {
                JsonObject jsonObject = (JsonObject) fromObj;
                Map<String,String> mapToReturn =new HashMap<String,String>();
                for (Map.Entry<String, Object> entry : jsonObject.getMap().entrySet()) {
                    mapToReturn.put(entry.getKey(), (String) entry.getValue());
                }
                return mapToReturn;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid Map<String,String> !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a Map<String,String> !",
                        pointer, fromObj));
    }

    public static List<String> parseListString(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (fromObj instanceof JsonArray) {
            try {
                JsonArray jsonArray = (JsonArray) fromObj;
                return jsonArray.stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid List<String> !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a List<String> !",
                        pointer, fromObj));
    }

    public static String parseString(JsonObject requestBody, String pointer) {
        return parse(requestBody, pointer, String.class);
    }

    public static QualityAgg parseQualityAggregation(JsonObject requestBody, String qualityAggPath) {
        LOGGER.debug("trying to parse pointer {}", qualityAggPath);
        JsonPointer jsonPointer = JsonPointer.from(qualityAggPath);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        try {
            return QualityAgg.valueOf(fromObj.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(fromObj.toString()+ " is not a recognized aggregation ");
        }
    }

    public static Float parseFloat(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (fromObj instanceof Number) { // TODO check this !
            try {
                return convertToFloat(fromObj);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid Float !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not of class Float !",
                        pointer, fromObj));
    }

    public static Boolean parseBoolean(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (fromObj instanceof Boolean) {
            try {
                return (Boolean) fromObj;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid Boolean !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not of class Boolean !",
                        pointer, fromObj));
    }

    public static Float convertToFloat(Object value) {
        Double doubleValue = (Double) value;
        return doubleValue.floatValue();
    }

    public static <T> T parse(JsonObject requestBody, String pointer, Class<T> clazz) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (clazz.isInstance(fromObj)) {
            try {
                return clazz.cast(fromObj);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid %s !",
                                pointer, fromObj, clazz), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not of class %s !",
                        pointer, fromObj, clazz));
    }

    public static List<AGG> parseListAGG(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj == null)
            return null;
        if (fromObj instanceof JsonArray) {
            try {
                JsonArray jsonArray = (JsonArray) fromObj;
                return jsonArray.stream()
                        .map(i -> {
                            try {
                                return AGG.valueOf(i.toString());
                            } catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException(i.toString()+ " is not a recognized aggregation, the accepted aggregations are : " + Arrays.toString(values()));
                            }
                        })
                        .collect(Collectors.toList());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid List<AGG> !",
                                pointer, fromObj), e);
            }
        }
        if (fromObj instanceof Boolean) {
            try {
                boolean aggregBoolean = (boolean) fromObj;
                if(aggregBoolean)
                    return DEFAULT_ALL_AGGREGATION_LIST;
                else
                    throw new IllegalArgumentException("aggregations value could not be other then true");
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' is not a valid value here !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a List<String> !",
                        pointer, fromObj));
    }

}
