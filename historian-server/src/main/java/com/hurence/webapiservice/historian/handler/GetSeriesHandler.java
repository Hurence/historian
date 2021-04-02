package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.webapiservice.QueryParamLookup;
import com.hurence.webapiservice.historian.SolrHistorianConf;
import com.hurence.webapiservice.util.StringParserUtils;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.*;

public class GetSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetSeriesHandler.class);
    SolrHistorianConf solrHistorianConf;

    public GetSeriesHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonArray>> getHandler(JsonObject params) {

        return p -> {
            try {

                // first step : get all possible tag names
                String synonymName= params.getString(SOLR_COLUMN_NAME);
                String originalName = QueryParamLookup.getOriginal( synonymName);


                // second step facet on those tags names to get possible values
                JsonArray data = new JsonArray();
                LOGGER.debug("looking up tags for originalName : {}", originalName);
                Map<String, String> synonymsTags = QueryParamLookup.getSwappedTagsBySynonymName(synonymName);
                if (synonymsTags != null && !synonymsTags.isEmpty()) {
                    synonymsTags.keySet().forEach(tag -> data.add(
                            new JsonObject()
                                    .put(__NAME__, synonymName)
                                    .put(synonymsTags.get(tag), tag))
                    );
                    LOGGER.debug("found tags in synonyms :{}", data.encodePrettily());
                } else {
                    LOGGER.debug("looking for tags in solr :{}", synonymName);
                    final SolrQuery tagNamesQuery = buildTagNamesQuery(params);
                    final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, tagNamesQuery);
                    List<SolrDocument> solrDocuments = new ArrayList<>(response.getResults());


                    Set<String> tagNames = new HashSet<>();
                    solrDocuments.forEach(doc -> {
                        tagNames.addAll(
                                StringParserUtils.extractTagsValues(
                                        doc.getFieldValue(SOLR_COLUMN_METRIC_KEY)
                                                .toString()
                                                .replaceFirst(synonymName + "\\|", "")
                                )
                        );
                    });
                    for (String tagName : tagNames) {
                        final SolrQuery tagValuesQuery = buildTagValuesQuery(params, tagName);
                        final QueryResponse tagValuesResponse = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, tagValuesQuery);
                        FacetField facetField = tagValuesResponse.getFacetField(tagName);
                        if (facetField == null || facetField.getValues() == null) {
                            LOGGER.warn("unable to retrieve any labels from facet : {}", tagValuesQuery.toQueryString());
                        } else {
                            facetField.getValues().forEach(count -> data.add(
                                    new JsonObject()
                                            .put(__NAME__, synonymName)
                                            .put(tagName, count.getName()))
                            );
                        }
                    }
                }

                // build json response
                p.complete(data);

            } catch (IOException | SolrServerException e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }

    /**
     * try to retrieve possible tags for a metric name in a given time range
     *
     * @param params
     * @return
     */
    private SolrQuery buildTagNamesQuery(JsonObject params) {

        // compute date range query
        Long from = params.getLong(HistorianServiceFields.FROM);
        Long to = params.getLong(HistorianServiceFields.TO);
        String dateRange = String.format("%s:[* TO %d] AND %s:[%d TO *]", SOLR_COLUMN_START, to, SOLR_COLUMN_END, from);

        // compute tags filter query
        SolrQuery query = new SolrQuery();
        query.setQuery(dateRange);
        query.setFilterQueries(params.getString(LUCENE_QUERY));
        query.setRows(20);
        query.setFields(SOLR_COLUMN_METRIC_KEY);
        LOGGER.info("TagNamesQuery : {}", query.toQueryString());

        return query;
    }

    /**
     * try to retrieve possible values for a metric name in a given time range
     * for the given tags
     *
     * @param params
     * @return
     */
    private SolrQuery buildTagValuesQuery(JsonObject params, String tagName) {

        // buld final query
        SolrQuery query = buildTagNamesQuery(params);
        query.setRows(0);
        query.setFacet(true);
        query.setFacetMinCount(1);
        query.setFacetLimit(50);
        query.addFacetField(tagName);

        LOGGER.info("TagValuesQuery : {}", query.toQueryString());

        return query;
    }
}
