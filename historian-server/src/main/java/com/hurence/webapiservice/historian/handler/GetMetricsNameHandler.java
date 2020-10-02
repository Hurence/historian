package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianConf;
import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.SolrHistorianConf;
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

import static com.hurence.historian.modele.HistorianServiceFields.METRICS;

public class GetMetricsNameHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetMetricsNameHandler.class);
    HistorianConf historianConf;
    SolrHistorianConf solrHistorianConf;

    public GetMetricsNameHandler(HistorianConf historianConf, SolrHistorianConf solrHistorianConf) {
        this.historianConf = historianConf;
        this.solrHistorianConf = solrHistorianConf;
    }

    private SolrFieldMapping getHistorianFields() {
        return this.historianConf.getFieldsInSolr();
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {

        String name = params.getString(HistorianServiceFields.METRIC);
        String queryString =  getHistorianFields().CHUNK_NAME +":*";
        if (name!=null && !name.isEmpty()) {
            queryString =  getHistorianFields().CHUNK_NAME + ":*" + name + "*";
        }
        SolrQuery query = new SolrQuery(queryString);
        query.setFilterQueries(queryString);
        int max = params.getInteger(HistorianServiceFields.LIMIT, solrHistorianConf.maxNumberOfTargetReturned);
        query.setRows(0);//we only need distinct values of metrics
        query.setFacet(true);
//        query.setFacetSort("index");
//        query.setFacetPrefix("per");
        query.setFacetLimit(max);
        query.setFacetMinCount(1);//number of doc matching the query is at least 1
        query.addFacetField( getHistorianFields().CHUNK_NAME);
        //  EXECUTE REQUEST
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);
                FacetField facetField = response.getFacetField( getHistorianFields().CHUNK_NAME);
                List<FacetField.Count> facetFieldsCount = facetField.getValues();
                if (facetFieldsCount.size() == 0) {
                    p.complete(new JsonObject()
                            .put(HistorianServiceFields.TOTAL, 0)
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
                        .put(HistorianServiceFields.TOTAL, facetField.getValueCount())
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
