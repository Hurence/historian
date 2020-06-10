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

public class GetMetricsNameRequestHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetMetricsNameRequestHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetMetricsNameRequestHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {

        String name = params.getString(HistorianFields.METRIC);
        String queryString = NAME +":*";
        if (name!=null && !name.isEmpty()) {
            queryString = NAME + ":*" + name + "*";
        }
        SolrQuery query = new SolrQuery(queryString);
        query.setFilterQueries(queryString);
        int max = params.getInteger(LIMIT, solrHistorianConf.maxNumberOfTargetReturned);
        query.setRows(0);//we only need distinct values of metrics
        query.setFacet(true);
//        query.setFacetSort("index");
//        query.setFacetPrefix("per");
        query.setFacetLimit(max);
        query.setFacetMinCount(1);//number of doc matching the query is at least 1
        query.addFacetField(NAME);
        //  EXECUTE REQUEST
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);
                FacetField facetField = response.getFacetField(NAME);
                List<FacetField.Count> facetFieldsCount = facetField.getValues();
                if (facetFieldsCount.size() == 0) {
                    p.complete(new JsonObject()
                            .put(TOTAL, 0)
                            .put(METRICS, new JsonArray())
                    );
                    return;
                }
                LOGGER.debug("Found " + facetField.getValueCount() + " different values");
                JsonArray metrics = new JsonArray(facetFieldsCount.stream()
                        .map(FacetField.Count::getName)
                        .collect(Collectors.toList())
                );
                p.complete(new JsonObject()
                        .put(TOTAL, facetField.getValueCount())
                        .put(METRICS, metrics)
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
