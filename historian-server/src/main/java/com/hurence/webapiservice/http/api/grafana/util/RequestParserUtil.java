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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

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
            return null;
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
                        .map(i -> AGG.valueOf(i.toString()))
                        .collect(Collectors.toList());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        String.format("'%s' json pointer value '%s' could not be cast as a valid List<AGG> !",
                                pointer, fromObj), e);
            }
        }
        throw new IllegalArgumentException(
                String.format("'%s' json pointer value '%s' is not a List<String> !",
                        pointer, fromObj));
    }

}
