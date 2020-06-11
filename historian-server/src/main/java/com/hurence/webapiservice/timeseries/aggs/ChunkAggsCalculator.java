package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.DoubleStream;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

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
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_COUNT_FIELD))
                .sum();
    }

}
