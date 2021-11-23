package com.hurence.webapiservice.http.api.grafana.promql.converter;

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
        return new JsonObject()
                .put(FROM, request.getStart())
                .put(TO, request.getEnd())
                .put(NAME, request.getName())
                .put(MATCH, request.getMatch());
    }

    public static JsonObject toGetSeriesParameters(SeriesRequest request) {
            return new JsonObject()
                    .put(FROM, request.getStart())
                    .put(TO, request.getEnd())
                    .put(SOLR_COLUMN_NAME, request.getMatchParameter().getName())
                    .put(LUCENE_QUERY, request.getMatchParameter().getLuceneQuery())
                    .put(LIMIT, 30)
                    .put(MATCH_ANY, true)
                    .put(TYPE, AnnotationRequestType.TAGS);
    }

    public static JsonObject toGetTimeSeriesRequest(QueryRequest request) throws IllegalArgumentException {
        QueryParameter qp = PromQLSynonymLookup.getOriginal(request.getQuery());
        if(!request.getQuery().getName().equals( qp.getName()))
            LOGGER.info("found synonym \"{}\" for metric \"{}\" in lookup table", qp.getName() ,request.getQuery());
        else
            LOGGER.info("synonym not found for metric \"{}\" in lookup table", request.getQuery());

        // build other parameters from
        // @TODO add quality and aggregation metrics
        return new JsonObject()
                .put(FROM, request.getStart())
                .put(TO, request.getEnd())
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

}
