package com.hurence.webapiservice.http.api.grafana.promql.converter;

import com.hurence.webapiservice.QueryParamLookup;
import com.hurence.webapiservice.http.api.grafana.model.AnnotationRequestType;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import com.hurence.webapiservice.http.api.grafana.promql.request.LabelsRequest;
import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import com.hurence.webapiservice.http.api.grafana.promql.request.SeriesRequest;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.SOLR_COLUMN_NAME;

/**
 * this class is useful to convert PromQL request JsonObjects into Historian handler parameters
 *
 * @see com.hurence.webapiservice.historian.handler
 */
public class RequestConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestConverter.class);

    public static JsonObject toGetLabelsParameters(LabelsRequest request) {

       /* QueryParamLookup.enable();
        QueryParamLookup.put("METEO.CENTRALE_METEO.MET_sunshine", "sunshine{type=METEO}");
        QueryParamLookup.put("METEO.CENTRALE_METEO.MET_humidity", "humidity{type=METEO}");
        QueryParamLookup.put("messages_ready", "messages_ready_renamed");

        QueryParamLookup.put("cpu_prct_used{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}", "cpu_prct_used_renamed{metric_id=ac8519bc-2985-4869-8aa8-c5fdfc68ec44}");
        QueryParamLookup.put("cpu_prct_used{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}", "cpu_prct_used_renamed{metric_id=ae320972-f612-4cef-8570-908250b8bd1a}");
        QueryParamLookup.put("cpu_prct_used{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}", "cpu_prct_used_renamed{metric_id=af33574c-c852-4b76-9e67-b19ad444250d}");
        QueryParamLookup.put("cpu_prct_used{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}", "cpu_prct_used_renamed{metric_id=b2c2b01e-2c54-4d05-9687-b69cab6cfc68}");

*/
        return new JsonObject()
                .put(FROM, request.getStart()*1000)
                .put(TO, request.getEnd()*1000);


    }

    public static JsonObject toGetSeriesParameters(SeriesRequest request) {


            return new JsonObject()
                    .put(FROM, request.getStart()*1000)
                    .put(TO, request.getEnd()*1000)
                    .put(SOLR_COLUMN_NAME, request.getMatchParameter().getName())
                    .put(LUCENE_QUERY, request.getMatchParameter().getLuceneQuery())
                    .put(LIMIT, 30)
                    .put(MATCH_ANY, true)
                    .put(TYPE, AnnotationRequestType.TAGS);


    }

    public static JsonObject toGetTimeSeriesRequest(QueryRequest request) throws IllegalArgumentException {

        QueryParameter qp = QueryParamLookup.getOriginal(request.getQuery());
        LOGGER.info("synonym lookup {} -> {}",request.getQuery().getName(), qp.getName() );



        // build other parameters from
        // @TODO add quality and aggregation metrics
        return new JsonObject()
                .put(FROM, request.getStart()*1000)
                .put(TO, request.getEnd()*1000)
                .put(NAMES,  Collections.singletonList(qp.getName()))
                .put(TAGS, qp.getTags())
                .put(SAMPLING_ALGO, request.getQuery().getSampling().getAlgo())
                .put(BUCKET_SIZE, request.getQuery().getSampling().getBucketSize())
                .put(MAX_POINT_BY_METRIC, request.getQuery().getSampling().getMaxPoint())
               /* .put(AGGREGATION, requests.get(0).getQuery().getAggregationOperator()..collect(Collectors.toList())
                .put(QUALITY_VALUE, request.getQualityValue())
                .put(QUALITY_AGG, request.getQualityAgg().toString())
                .put(QUALITY_RETURN, request.getQuery().getQuality())
                .put(USE_QUALITY, request.getUseQuality())*/;
    }
/*

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
