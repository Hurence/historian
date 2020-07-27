package com.hurence.webapiservice.historian.compatibility;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaCompatibilityUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(JsonSchemaCompatibilityUtil.class);

    /**
     * This methode do modify input.
     * @param chunk
     * @return
     */
    public static JsonObject convertJsonSchemaEVOA0ToVERSION_0(JsonObject chunk) {
        LOGGER.debug("chunk version {} is {}", SchemaVersion.EVOA0, chunk);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY, HistorianFields.CHUNK_DAY);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, HistorianFields.CHUNK_MONTH);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, HistorianFields.CHUNK_YEAR);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG, HistorianFields.CHUNK_AVG_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST, HistorianFields.CHUNK_FIRST_VALUE_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM, HistorianFields.CHUNK_SUM_FIELD);
        convertMultiValuedFieldToMonoValued(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND, HistorianFields.CHUNK_TREND_FIELD);
        LOGGER.debug("chunk version {} (converted) is {}", SchemaVersion.VERSION_0, chunk);
        return chunk;
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
