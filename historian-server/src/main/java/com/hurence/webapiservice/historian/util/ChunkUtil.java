package com.hurence.webapiservice.historian.util;

import com.hurence.historian.modele.HistorianFields;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class ChunkUtil {

    private ChunkUtil() {}

    public static int countTotalNumberOfPointInChunks(List<JsonObject> chunks) throws UnsupportedOperationException {
        return chunks.stream()
                .mapToInt(chunk -> chunk.getInteger(HistorianFields.RESPONSE_CHUNK_COUNT_FIELD))
                .sum();
    }
}
