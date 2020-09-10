package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class GetFieldValuesHandler {
    private static Logger LOGGER = LoggerFactory.getLogger(GetFieldValuesHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetFieldValuesHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {
        String field = params.getString(HistorianServiceFields.FIELD);
        String query = params.getString(HistorianServiceFields.QUERY);
        int limit = params.getInteger(HistorianServiceFields.LIMIT, solrHistorianConf.maxNumberOfTargetReturned);
        String queryString =  ClientUtils.escapeQueryChars(field) +":*";
        if (query!=null && !query.isEmpty()) {
            queryString = ClientUtils.escapeQueryChars(field) + ":*" + ClientUtils.escapeQueryChars(query) + "*";
        }
        SolrQuery solrQuery = new SolrQuery(queryString);
        solrQuery.setFilterQueries(queryString);
        solrQuery.setRows(0);
        solrQuery.setFacet(true);
        solrQuery.setFacetLimit(limit);
        solrQuery.setFacetMinCount(1);
        solrQuery.addFacetField(field);
        LOGGER.trace("solrQuery is :\"{}\".", solrQuery);
        //  EXECUTE REQUEST
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, solrQuery);
                FacetField facetField = response.getFacetField(field);
                List<FacetField.Count> facetFieldsCount = facetField.getValues();
                if (facetFieldsCount.size() == 0) {
                    p.complete(new JsonObject()
                            .put(HistorianServiceFields.TOTAL, 0)
                            .put(HistorianServiceFields.RESPONSE_VALUES, new JsonArray())
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
                        .put(HistorianServiceFields.TOTAL, facetField.getValueCount())
                        .put(HistorianServiceFields.RESPONSE_VALUES, metrics)
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
