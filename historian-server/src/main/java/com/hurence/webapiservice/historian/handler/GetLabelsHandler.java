package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.webapiservice.http.api.grafana.promql.converter.PromQLSynonymLookup;
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
import java.util.HashSet;
import java.util.Set;

import static com.hurence.timeseries.model.Definitions.*;

public class GetLabelsHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetLabelsHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetLabelsHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonArray>> getHandler(JsonObject params) {

        return p -> {
            try {

                LOGGER.debug("getting labels with parameters : {}", params.encode());
                // compute date range query
                StringBuilder queryBuilder = new StringBuilder();
                Long from = params.getLong(HistorianServiceFields.FROM);
                Long to = params.getLong(HistorianServiceFields.TO);

                String dateRange = "*:*";
                if(from != null && to != null){
                    dateRange = String.format("%s:[* TO %d] AND %s:[%d TO *]", SOLR_COLUMN_START, to, SOLR_COLUMN_END, from);
                }else if(from != null){
                    dateRange = String.format("%s:[%d TO *]", SOLR_COLUMN_END, from);
                }else if(to != null){
                    dateRange = String.format("%s:[* TO %d]", SOLR_COLUMN_START, to);
                }

                final SolrQuery query = new SolrQuery();
                query.setQuery(dateRange);
                query.setRows(0);
                query.setFacet(true);
                query.setFacetMinCount(1);
                query.setFacetLimit(2000);
                query.addFacetField(SOLR_COLUMN_NAME);
                query.setFacetSort("index");

                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);

                // second step facet on those tags names to get possible values
                JsonArray data = new JsonArray();
                Set<String> uniqueSynonymNames = new HashSet<>();

                FacetField facetField = response.getFacetField(SOLR_COLUMN_NAME);
                if (facetField == null || facetField.getValues() == null) {
                    LOGGER.warn("unable to retrieve any labels from facet : {}", query.toQueryString());
                } else {
                    facetField.getValues().forEach(count -> uniqueSynonymNames.add(PromQLSynonymLookup.getSynonymName(count.getName())));
                    uniqueSynonymNames.forEach(data::add);
                }



                // build json response
                p.complete(data);

            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }

}
