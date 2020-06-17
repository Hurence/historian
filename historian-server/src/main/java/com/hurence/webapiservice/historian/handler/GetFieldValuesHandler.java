package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.historian.modele.HistorianFields.METRICS;
import static com.hurence.webapiservice.http.api.main.modele.QueryFields.*;

public class GetFieldValuesHandler {
    private static Logger LOGGER = LoggerFactory.getLogger(GetMetricsNameHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetFieldValuesHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {

        String field = params.getString(QUERY_PARAM_FIELD);
        String query = params.getString(QUERY_PARAM_QUERY);
        int limit = params.getInteger(QUERY_PARAM_LIMIT, solrHistorianConf.maxNumberOfTargetReturned);
        String queryString = field +":*";
        if (query!=null && !query.isEmpty()) {
            queryString = field + ":*" + query + "*";
        }
        SolrQuery solrQuery = new SolrQuery(queryString);
        solrQuery.setFilterQueries(queryString);
        solrQuery.setRows(0);//we only need distinct values of metrics
        solrQuery.setFacet(true);
//        query.setFacetSort("index");
//        query.setFacetPrefix("per");
        solrQuery.setFacetLimit(limit);
        solrQuery.setFacetMinCount(1);//number of doc matching the query is at least 1
        solrQuery.addFacetField(field);
        //  EXECUTE REQUEST
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, solrQuery);
                FacetField facetField = response.getFacetField(field);
                List<FacetField.Count> facetFieldsCount = facetField.getValues();
                if (facetFieldsCount.size() == 0) {
                    p.complete(new JsonObject()
                            .put(TOTAL, 0)
                            .put(QUERY_PARAM_VALUES, new JsonArray())
                    );
                    return;
                }
                LOGGER.debug("Found " + facetField.getValueCount() + " different values");
                JsonArray metrics = new JsonArray(facetFieldsCount.stream()
                        .map(FacetField.Count::getName)
                        .sorted()
                        .collect(Collectors.toList())
                );
                p.complete(new JsonObject()
                        .put(TOTAL, facetField.getValueCount())
                        .put(QUERY_PARAM_VALUES, metrics)
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }
}
