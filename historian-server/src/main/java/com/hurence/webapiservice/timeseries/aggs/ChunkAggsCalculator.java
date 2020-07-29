package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.historian.modele.FieldNamesInsideHistorianService;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.util.*;

import java.util.stream.DoubleStream;

public class ChunkAggsCalculator extends AbstractAggsCalculator<JsonObject> {

    public ChunkAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected DoubleStream getDoubleStreamFromElementsToAgg(List<JsonObject> chunks, String field) {
        return chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(field));
    }

    @Override
    protected double getDoubleCount(List<JsonObject> chunks) {
        return chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(FieldNamesInsideHistorianService.CHUNK_COUNT))
                .sum();
    }

}
