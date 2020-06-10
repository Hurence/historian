package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class GetTimeSeriesChunkHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesChunkHandler.class);
    SolrHistorianConf solrHistorianConf;


    public GetTimeSeriesChunkHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {
        final SolrQuery query = buildTimeSeriesChunkQuery(params);
        //    FILTER
        buildSolrFilterFromTags(params.getJsonObject(HistorianFields.TAGS))
                .ifPresent(query::addFilterQuery);
        query.setFields();//so we return every fields (this endpoint is currently used only in tests, this is legacy code)
        //  EXECUTE REQUEST
        return p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);
                final SolrDocumentList documents = response.getResults();
                LOGGER.debug("Found " + documents.getNumFound() + " documents");
                JsonArray docs = new JsonArray(documents.stream()
                        .map(this::convertDoc)
                        .collect(Collectors.toList())
                );
                p.complete(new JsonObject()
                        .put(HistorianFields.TOTAL, documents.getNumFound())
                        .put(CHUNKS, docs)
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }

    private Optional<String> buildSolrFilterFromTags(JsonObject tags) {
        if (tags == null || tags.isEmpty())
            return Optional.empty();
        String filters = tags.fieldNames().stream()
                .map(tagName -> {
                    String value = tags.getString(tagName);
                    return tagName + ":\"" + value + "\"";
//                    return "\"" + tagName + "\":\"" + value + "\"";
                })
                .collect(Collectors.joining(" AND ", "", ""));
        return Optional.of(filters);
    }


    private JsonObject convertDoc(SolrDocument doc) {
        final JsonObject json = new JsonObject();
        doc.getFieldNames().forEach(f -> {
            Object value = doc.get(f);
            if (value instanceof Date) {
                value = value.toString();
            }
            if (value != null && value instanceof List) {
                List<Object> newListWithoutDate = new ArrayList<>();
                for (Object elem : (List<Object>) value) {
                    if (elem instanceof Date) {
                        newListWithoutDate.add(elem.toString());
                    } else {
                        newListWithoutDate.add(elem);
                    }
                }
                value = newListWithoutDate;
            }
            json.put(f, value);
        });
        return json;
    }


    private SolrQuery buildTimeSeriesChunkQuery(JsonObject params) {
        StringBuilder queryBuilder = new StringBuilder();
        if (params.getLong(TO) != null) {
            LOGGER.trace("requesting timeseries to {}", params.getLong(TO));
            queryBuilder.append(RESPONSE_CHUNK_START_FIELD).append(":[* TO ").append(params.getLong(TO)).append("]");
        }
        if (params.getLong(FROM) != null) {
            LOGGER.trace("requesting timeseries from {}", params.getLong(FROM));
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(RESPONSE_CHUNK_END_FIELD).append(":[").append(params.getLong(FROM)).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());
        //TODO filter on names AND tags
//        buildSolrFilterFromArray(params.getJsonArray(NAMES), NAME)
//                .ifPresent(query::addFilterQuery);
        //    FIELDS_TO_FETCH
        query.setFields(RESPONSE_CHUNK_START_FIELD,
                RESPONSE_CHUNK_END_FIELD,
                RESPONSE_CHUNK_COUNT_FIELD,
                NAME);
        //    SORT
        query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(RESPONSE_CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000));
        return query;
    }
}
