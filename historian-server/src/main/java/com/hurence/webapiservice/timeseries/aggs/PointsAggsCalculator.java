package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PointsAggsCalculator implements AggsCalculator<Point> {

    private final List<AGG> askedAggList;
    private final List<AGG> neededAggList;
    private final Map<AGG, Number> aggValues = new HashMap<>();

    public PointsAggsCalculator(List<AGG> aggregList) {
        this.askedAggList = aggregList;
        this.neededAggList = calculNeededAggList(aggregList);
    }

    /**
     *
     * @param aggregList
     * @return list of aggs needed to be calculated. Indeed some aggs can be calculated using others.
     */
    private List<AGG> calculNeededAggList(List<AGG> aggregList) {
        //TODO
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

    /**
     * add Avg If needed
     */
    private void calculAvgIfNeeded() {
        //TODO
    }

    public void updateAggs(List<Point> points) {
        neededAggList.forEach(agg -> {
            switch (agg) {
                case SUM:
                    calculateSum(points);
                    break;
                case MIN:
                    calculateMin(points);
                    break;
                case MAX:
                    calculateMax(points);
                    break;
                case COUNT:
                    calculateCount(points);
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
    }

    /**
     * update sum value to aggValues
     */
    private void calculateSum(List<Point> points) {
        //TODO do not use for loop, instead user List/Stream api.
    }

    /**
     * update min value to aggValues
     */
    private void calculateMin(List<Point> points) {
        //TODO do not use for loop, instead user List/Stream api. use java.lang.Math library
    }
    private void calculateMax(List<Point> points) {
        //TODO do not use for loop, instead user List/Stream api. use java.lang.Math library
    }
    private void calculateCount(List<Point> points) {
        //TODO do not use for loop, instead user List/Stream api.
    }
}
