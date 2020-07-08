package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.util.*;

import java.util.stream.DoubleStream;

import static com.hurence.historian.modele.HistorianFields.*;

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
                .mapToDouble(chunk -> chunk.getDouble(CHUNK_COUNT_FIELD))
                .sum();
    }

}
