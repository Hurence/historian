package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class ChunkAggsCalculator implements AggsCalculator<JsonObject> {

    private final List<AGG> askedAggList;
    private final List<AGG> neededAggList;
    private final Map<AGG, Number> aggValues = new HashMap<>();


    public ChunkAggsCalculator(List<AGG> aggregList) {
        this.askedAggList = aggregList;
        this.neededAggList = calculNeededAggList(aggregList);
    }

    private List<AGG> calculNeededAggList(List<AGG> aggregList) {
        return null;
    }

    /**
     *
     * @return The aggs as a Json object if there is any requested otherwise return empty.
     */
    public Optional<JsonObject> getAggsAsJson() {
        calculAvgIfNeeded();
        return null;//TODO
    }

    private void calculAvgIfNeeded() {
        //TODO
    }

    /**
     * update aggs values for whose that are cumulative (ie not avg)
     * @param chunks
     */
    public void updateAggs(List<JsonObject> chunks) {
        neededAggList.forEach(agg -> {
            switch (agg) {
                case SUM:
                    calculateSum(chunks);
                    break;
                case MIN:
                    calculateMin(chunks);
                    break;
                case MAX:
                    calculateMax(chunks);
                    break;
                case COUNT:
                    calculateCount(chunks);
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
    }

    /**
     * update sum value to aggValues
     */
    private void calculateSum(List<JsonObject> chunks) {
        //TODO do not use for loop, instead user List/Stream api.
    }

    /**
     * update min value to aggValues
     */
    private void calculateMin(List<JsonObject> chunks) {
        //TODO do not use for loop, instead user List/Stream api. use java.lang.Math library
    }
    private void calculateMax(List<JsonObject> chunks) {
        //TODO do not use for loop, instead user List/Stream api. use java.lang.Math library
    }
    private void calculateCount(List<JsonObject> chunks) {
        //TODO do not use for loop, instead user List/Stream api.
    }

}
