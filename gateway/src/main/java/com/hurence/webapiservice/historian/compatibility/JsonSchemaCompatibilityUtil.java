package com.hurence.webapiservice.historian.compatibility;

import com.hurence.historian.modele.HistorianFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.List;

public class JsonSchemaCompatibilityUtil {

    public static final List<String> multivaluedListFieldsToConvertToMonoField = Arrays.asList(
            HistorianFields.CHUNK_DAY,
            HistorianFields.CHUNK_MONTH,
            HistorianFields.CHUNK_YEAR,
            HistorianFields.RESPONSE_CHUNK_AVG_FIELD,
            HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD,
            HistorianFields.RESPONSE_CHUNK_SUM_FIELD,
            HistorianFields.RESPONSE_CHUNK_TREND_FIELD
    );

    public static JsonObject convertSchema0ToCurrent(JsonObject chunk) {
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
        return chunk;
    }
}
