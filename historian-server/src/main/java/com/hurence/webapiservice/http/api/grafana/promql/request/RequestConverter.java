package com.hurence.webapiservice.http.api.grafana.promql.request;

import com.hurence.webapiservice.http.api.grafana.model.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.modele.AnnotationRequest;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.util.stream.Collectors;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;

/**
 * this class is useful to convert PromQL request JsonObjects into Historian handler parameters
 *
 * @see com.hurence.webapiservice.historian.handler
 */
public class RequestConverter {

    public static JsonObject toGetMetricsNameParameters(SeriesRequest request){
        JsonObject parameters = new JsonObject();

        return parameters;
    }
/*
    public static JsonObject toGetTimeSeriesRequest(QueryRequest request) {

        return new JsonObject()
                .put(FROM, request.getStart())
                .put(TO, request.getEnd())
                .put(NAMES, request.getMetricNames())
                .put(FIELD_TAGS, request.getTags())
                .put(SAMPLING_ALGO, request.)
                .put(BUCKET_SIZE, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC, samplingConf.getMaxPoint())
                .put(AGGREGATION, request.getAggs().stream().map(String::valueOf).collect(Collectors.toList()))
                .put(QUALITY_VALUE, request.getQualityValue())
                .put(QUALITY_AGG, request.getQualityAgg().toString())
                .put(QUALITY_RETURN, request.getQualityReturn())
                .put(USE_QUALITY, request.getUseQuality());
    }

    public static JsonObject toHistorianAnnotationRequest(AnnotationRequest request) {
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(FIELD_TAGS, request.getTags())
                .put(LIMIT, request.getMaxAnnotation())
                .put(MATCH_ANY, request.getMatchAny())
                .put(TYPE, request.getType());
    }*/
}
