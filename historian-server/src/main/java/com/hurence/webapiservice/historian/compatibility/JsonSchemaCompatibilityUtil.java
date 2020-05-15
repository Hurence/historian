package com.hurence.webapiservice.historian.compatibility;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class JsonSchemaCompatibilityUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(JsonSchemaCompatibilityUtil.class);

    public static void convertJsonSchemaEVOA0ToVERSION_0(JsonObject chunk) {
        LOGGER.debug("chunk version {} is {}", SchemaVersion.EVOA0, chunk);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY, HistorianFields.CHUNK_DAY);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, HistorianFields.CHUNK_MONTH);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, HistorianFields.CHUNK_YEAR);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG, HistorianFields.RESPONSE_CHUNK_AVG_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST, HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM, HistorianFields.RESPONSE_CHUNK_SUM_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND, HistorianFields.RESPONSE_CHUNK_TREND_FIELD);
        LOGGER.debug("chunk version {} (converted) is {}", SchemaVersion.VERSION_0, chunk);
    }

    private static void convertMultiValuedFieldToMonoValued(JsonObject chunk,
                                                            String fieldNameToConvert,
                                                            String fieldNameConverted) {
        if (chunk.containsKey(fieldNameToConvert)) {
            Object value = chunk.getValue(fieldNameToConvert);
            if (value instanceof JsonArray) {
                JsonArray array = chunk.getJsonArray(fieldNameToConvert);
                if (array != null && !array.isEmpty()) {
                    chunk.put(fieldNameConverted, array.getValue(0));
                }
            } else {
                chunk.remove(fieldNameToConvert);
                chunk.put(fieldNameConverted, value);
            }
        }
    }
}
