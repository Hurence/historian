package com.hurence.webapiservice.timeseries;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiTimeSeriesExtracterImpl implements MultiTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(MultiTimeSeriesExtracterImpl.class);

    private Map<String, TimeSeriesExtracter> bucketerByMetrics = new HashMap<>();
    Map<String, Long> totalNumberOfPointByMetrics = new HashMap<>();
    List<String> aggregList;
    final long from;
    final long to;
    final SamplingConf samplingConf;

    public MultiTimeSeriesExtracterImpl(long from, long to, SamplingConf samplingConf) {
        this.from = from;
        this.to = to;
        this.samplingConf = samplingConf;
    }


    @Override
    public void addChunk(JsonObject chunk) {
        String metricName = chunk.getString(HistorianFields.NAME);
        bucketerByMetrics
                .computeIfAbsent(metricName, this::createTimeSeriesExtractor)
                .addChunk(chunk);
    }

    @Override
    public void flush() {
        bucketerByMetrics.values()
                .forEach(TimeSeriesExtracter::flush);
    }

    /**
     * Calcule the aggregations. Add them into aggreg Values List
     */
    public void calculateAggreg() {
        bucketerByMetrics.values()
                .forEach(TimeSeriesExtracter::calculateAggreg);
    }

    @Override
    public void setAggregationList(JsonArray aggregationList) {
        aggregList = aggregationList.getList();
    }

    protected TimeSeriesExtracter createTimeSeriesExtractor(String metricName) {
        return new TimeSeriesExtracterImpl(metricName, from, to, samplingConf, totalNumberOfPointByMetrics.get(metricName), aggregList);
    }

    public void setTotalNumberOfPointForMetric(String metric, long totalNumberOfPoints) {
        totalNumberOfPointByMetrics.put(metric, totalNumberOfPoints);
    }

    @Override
    public JsonArray getTimeSeries() {
        List<JsonObject> timeseries = bucketerByMetrics.values().stream()
                .map(TimeSeriesExtracter::getTimeSeries)
                .collect(Collectors.toList());
        JsonArray toReturn = new JsonArray(timeseries);
        LOGGER.trace("getTimeSeries return : {}", toReturn.encodePrettily());
        return toReturn;
    }

    public JsonObject getAggregation() {
        return null;
    }

    @Override
    public long chunkCount() {
        return bucketerByMetrics.values().stream()
                .mapToLong(TimeSeriesExtracter::chunkCount)
                .sum();
    }

    @Override
    public long pointCount() {
        return bucketerByMetrics.values().stream()
                .mapToLong(TimeSeriesExtracter::pointCount)
                .sum();
    }
}
