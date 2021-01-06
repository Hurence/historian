package com.hurence.webapiservice.timeseries.aggs;

import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Optional;

public interface AggsCalculator<T> {

    Optional<JsonObject> getAggsAsJson();

    void updateAggs(List<T> elementsToAgg);
}
