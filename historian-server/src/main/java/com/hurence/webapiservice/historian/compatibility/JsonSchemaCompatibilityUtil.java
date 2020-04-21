package com.hurence.webapiservice.historian.compatibility;

import com.hurence.historian.spark.compactor.job.HistorianFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class JsonSchemaCompatibilityUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(JsonSchemaCompatibilityUtil.class);

    public static final List<String> multivaluedListFieldsToConvertToMonoField = Arrays.asList(
            HistorianFields.CHUNK_DAY,
            HistorianFields.CHUNK_MONTH,
            HistorianFields.CHUNK_YEAR,
            HistorianFields.RESPONSE_CHUNK_AVG_FIELD,
            HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD,
            HistorianFields.RESPONSE_CHUNK_SUM_FIELD,
            HistorianFields.RESPONSE_CHUNK_TREND_FIELD
    );

    public static void convertJsonSchema0ToCurrent(JsonObject chunk) {
        LOGGER.debug("chunk version {} is {}", SchemaVersion.VERSION_0, chunk);
        multivaluedListFieldsToConvertToMonoField.forEach(f -> {
            if (chunk.containsKey(f)) {
                Object value = chunk.getValue(f);
                if (value instanceof JsonArray) {
                    JsonArray array = chunk.getJsonArray(f);
                    if (array != null && !array.isEmpty()) {
                        chunk.put(f, array.getValue(0));
                    }
                }
            }
        });
        LOGGER.debug("chunk version {} (converted) is {}", SchemaVersion.CURRENT_VERSION, chunk);
    }
}
