package com.hurence.webapiservice.historian.models;

import com.hurence.historian.model.HistorianServiceFields;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class HistorianResponseHelper {

    private HistorianResponseHelper() {}

    public static List<JsonObject> extractChunks(JsonObject chunkResponse) throws UnsupportedOperationException {
        final long totalFound = chunkResponse.getLong(HistorianServiceFields.TOTAL);
        List<JsonObject> chunks = chunkResponse.getJsonArray(HistorianServiceFields.CHUNKS).stream()
                .map(JsonObject.class::cast)
                .collect(Collectors.toList());
        if (totalFound != chunks.size())
            //TODO add a test with more than 10 chunks then implement handling more than default 10 chunks of solr
            //TODO should we add initial number of chunk to fetch in query param ?
            throw new UnsupportedOperationException("not yet supported when matching more than "+
                    chunks.size() + " chunks (total found : " + totalFound +")");
        return chunks;
    }


}
