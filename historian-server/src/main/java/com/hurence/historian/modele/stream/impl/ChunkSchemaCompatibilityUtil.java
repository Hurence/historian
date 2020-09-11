package com.hurence.historian.modele.stream.impl;

import com.hurence.historian.modele.FieldNamesInsideHistorianService;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkSchemaCompatibilityUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(ChunkSchemaCompatibilityUtil.class);

    /**
     * This methode do modify input.
     * @param chunk
     * @return
     */
    public static JsonObject convertEVOA0ToInternalChunk(JsonObject chunk) {
        LOGGER.debug("chunk version {} is {}", SchemaVersion.EVOA0, chunk);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY, FieldNamesInsideHistorianService.CHUNK_DAY);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, FieldNamesInsideHistorianService.CHUNK_MONTH);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, FieldNamesInsideHistorianService.CHUNK_YEAR);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG, FieldNamesInsideHistorianService.CHUNK_AVG);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST, FieldNamesInsideHistorianService.CHUNK_FIRST);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM, FieldNamesInsideHistorianService.CHUNK_SUM);
        convertMultiValuedFieldToMonoValuedOrJustChangeName(chunk,
                HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND, FieldNamesInsideHistorianService.CHUNK_TREND);
        changeNameOfField(chunk, HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE, FieldNamesInsideHistorianService.CHUNK_COUNT);
        LOGGER.debug("chunk version {} (converted) is {}", SchemaVersion.VERSION_0, chunk);
        return chunk;
    }

    public static JsonObject convertVERSION_0ToInternalChunk(JsonObject chunk) {
        return chunk;
    }

    private static void convertMultiValuedFieldToMonoValuedOrJustChangeName(JsonObject chunk,
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

    private static void changeNameOfField(JsonObject chunk,
                                        String fieldNameToConvert,
                                        String fieldNameConverted) {
        if (chunk.containsKey(fieldNameToConvert)) {
            Object value = chunk.getValue(fieldNameToConvert);
            chunk.remove(fieldNameToConvert);
            chunk.put(fieldNameConverted, value);
        }
    }
}
