package com.hurence.webapiservice.timeseries.extractor;

        import com.hurence.logisland.record.Point;
        import com.hurence.webapiservice.modele.AGG;
        import com.hurence.webapiservice.modele.SamplingConf;
        import io.vertx.core.json.JsonArray;
        import io.vertx.core.json.JsonObject;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.util.*;
        import java.util.stream.Collectors;

        import static com.hurence.historian.modele.HistorianFields.REF_ID;

public class MultiTimeSeriesExtracterImpl implements MultiTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(MultiTimeSeriesExtracterImpl.class);

    private final List<MetricRequest> metricRequests;
    private Map<MetricRequest, TimeSeriesExtracter> extractorByMetricRequest;
    Map<MetricRequest, Long> totalNumberOfPointByMetrics = new HashMap<>();
    List<AGG> aggregList = new ArrayList<>();
    final long from;
    final long to;
    final SamplingConf samplingConf;

    public MultiTimeSeriesExtracterImpl(long from,
                                        long to,
                                        SamplingConf samplingConf,
                                        List<MetricRequest> metricRequests) {
        this.from = from;
        this.to = to;
        this.samplingConf = samplingConf;
        this.extractorByMetricRequest = new HashMap<>();
        this.metricRequests = metricRequests;
    }


    @Override
    public void addChunk(JsonObject chunk) {
        metricRequests.forEach(metricRequest -> {
            if (metricRequest.isChunkMatching(chunk)) {
                extractorByMetricRequest
                        .computeIfAbsent(metricRequest, this::createTimeSeriesExtractor)
                        .addChunk(chunk);
            }
        });
    }

    @Override
    public void flush() {
        extractorByMetricRequest.values()
                .forEach(TimeSeriesExtracter::flush);
    }


    public void setAggregationList(List<AGG> aggregationList) {
        aggregList.addAll(aggregationList);
    }

    protected TimeSeriesExtracter createTimeSeriesExtractor(MetricRequest metricRequest) {
        return new TimeSeriesExtracterImpl(from, to, samplingConf, totalNumberOfPointByMetrics.get(metricRequest), aggregList);
    }

    public void setTotalNumberOfPointForMetric(MetricRequest metric, long totalNumberOfPoints) {
        totalNumberOfPointByMetrics.put(metric, totalNumberOfPoints);
    }

    @Override
    public JsonArray getTimeSeries() {
        List<JsonObject> timeseries = extractorByMetricRequest.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(MetricRequest::getName)))
                .map(entry -> {
                    JsonObject timeSerie = new JsonObject();
                    if (entry.getKey().getRefId().isPresent())
                        timeSerie.put(REF_ID, entry.getKey().getRefId().get());
                    timeSerie.put(TIMESERIE_NAME, entry.getKey().getName());
                    if (!entry.getKey().getTags().isEmpty())
                        timeSerie.put(TIMESERIE_TAGS, entry.getKey().getTags());
                    timeSerie.mergeIn(entry.getValue().getTimeSeries());
                    return timeSerie;
                })
                .collect(Collectors.toList());
        JsonArray toReturn = new JsonArray(timeseries);
        LOGGER.trace("getTimeSeries return : {}", toReturn.encodePrettily());
        return toReturn;
    }

    @Override
    public long chunkCount() {
        return extractorByMetricRequest.values().stream()
                .mapToLong(TimeSeriesExtracter::chunkCount)
                .sum();
    }

    @Override
    public long pointCount() {
        return extractorByMetricRequest.values().stream()
                .mapToLong(TimeSeriesExtracter::pointCount)
                .sum();
    }
}
