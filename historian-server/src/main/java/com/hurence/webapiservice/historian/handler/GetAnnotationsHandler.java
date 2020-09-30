package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.SolrHistorianConf;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;

public class GetAnnotationsHandler {


    private static Logger LOGGER = LoggerFactory.getLogger(GetAnnotationsHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetAnnotationsHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {
        final SolrQuery query = buildAnnotationQuery(params);
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.annotationCollection, query);
                SolrDocumentList solrDocuments = response.getResults();
                LOGGER.debug("Found " + response.getRequestUrl() + response + " result" + solrDocuments);
                JsonArray annotation = new JsonArray(new ArrayList<>(solrDocuments)
                );
                LOGGER.debug("annotations found : "+ annotation);
                p.complete(new JsonObject()
                        .put(HistorianServiceFields.ANNOTATIONS, annotation)
                        .put(HistorianServiceFields.TOTAL_HIT, annotation.size())
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }

    private SolrQuery buildAnnotationQuery(JsonObject params) {
        StringBuilder queryBuilder = new StringBuilder();
        Long from = params.getLong(HistorianServiceFields.FROM);
        Long to = params.getLong(HistorianServiceFields.TO);
        if (to == null && from != null) {
            LOGGER.trace("requesting annotation with from time {}", from);
            queryBuilder.append(TIME).append(":[").append(from).append(" TO ").append("*]");
        } else if (from == null && to != null) {
            LOGGER.trace("requesting annotation with to time {}", to);
            queryBuilder.append(TIME).append(":[*").append(" TO ").append(to).append("]");
        } else if (from != null) {
            LOGGER.trace("requesting annotation with time from {} to time {}", from, to);
            queryBuilder.append(TIME).append(":[").append(from).append(" TO ").append(to).append("]");
        } else {
            LOGGER.trace("requesting annotation with all times existing");
            queryBuilder.append("*:*");
        }
        //FILTER
        List<String> tags = null;
        if (params.getJsonArray(FIELD_TAGS) != null)
            tags = params.getJsonArray(FIELD_TAGS).getList();
        StringBuilder stringQuery = new StringBuilder();
        String operator = "";
        SolrQuery query = new SolrQuery();
        switch (AnnotationRequestType.valueOf(params.getString(HistorianServiceFields.TYPE, AnnotationRequestType.ALL.toString()))) {
            case ALL:
                break;
            case TAGS:
                queryBuilder.append(" && ");
                if (!params.getBoolean(HistorianServiceFields.MATCH_ANY, true)) {
                    operator = " AND ";
                } else {
                    operator = " OR ";
                }
                for (String tag : tags.subList(0,tags.size()-1)) {
                    stringQuery.append(tag).append(operator);
                }
                stringQuery.append(tags.get(tags.size()-1));
                queryBuilder.append(FIELD_TAGS).append(":").append("(").append(stringQuery.toString()).append(")");
                break;
        }
        if (queryBuilder.length() != 0 ) {
            LOGGER.info("query is : {}", queryBuilder.toString());
            query.setQuery(queryBuilder.toString());
        }
        query.setRows(params.getInteger(HistorianServiceFields.LIMIT, 1000));
        //    FIELDS_TO_FETCH
        query.setFields(TIME,
                TIME_END_REQUEST_FIELD,
                TEXT,
                FIELD_TAGS);
        query.addSort("score", SolrQuery.ORDER.desc);
        query.addSort(TIME, SolrQuery.ORDER.desc);
        return query;
    }
}
