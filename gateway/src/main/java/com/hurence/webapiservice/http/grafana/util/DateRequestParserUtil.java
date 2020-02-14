package com.hurence.webapiservice.http.grafana.util;

import com.hurence.webapiservice.http.grafana.QueryRequestParser;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

public class DateRequestParserUtil {

    private DateRequestParserUtil () {};

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestParser.class);
    private static SimpleDateFormat dateFormat = createDateFormat();

    public static SimpleDateFormat createDateFormat() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        TimeZone myTimeZone = TimeZone.getTimeZone(ZoneId.of("UTC"));
        dateFormat.setTimeZone(myTimeZone);
        dateFormat.setLenient(false);
        return dateFormat;
    }

    public static long parseDate(JsonObject requestBody, String pointer) {
        LOGGER.debug("trying to parse pointer {}", pointer);
        JsonPointer jsonPointer = JsonPointer.from(pointer);
        Object fromObj = jsonPointer.queryJson(requestBody);
        if (fromObj instanceof String) {
            String fromStr = (String) fromObj;
            try {
                return dateFormat.parse(fromStr).getTime();
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
}
