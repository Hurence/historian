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

import static com.hurence.historian.modele.HistorianServiceFields.TO;
import static com.hurence.timeseries.model.Definitions.*;

public class GetTimeSeriesChunkHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesChunkHandler.class);
    HistorianConf historianConf;
    SolrHistorianConf solrHistorianConf;

    public GetTimeSeriesChunkHandler(HistorianConf historianConf, SolrHistorianConf solrHistorianConf) {
        this.historianConf = historianConf;
        this.solrHistorianConf = solrHistorianConf;
    }

    private SolrFieldMapping getHistorianFields() {
        return this.historianConf.getFieldsInSolr();
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject params) {
        final SolrQuery query = buildTimeSeriesChunkQuery(params);
        //    FILTER
        buildSolrFilterFromTags(params.getJsonObject(FIELD_TAGS))
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
                        .put(HistorianServiceFields.TOTAL, documents.getNumFound())
                        .put(HistorianServiceFields.CHUNKS, docs)
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
            queryBuilder.append(SOLR_COLUMN_START).append(":[* TO ").append(params.getLong(TO)).append("]");
        }
        if (params.getLong(HistorianServiceFields.FROM) != null) {
            LOGGER.trace("requesting timeseries from {}", params.getLong(HistorianServiceFields.FROM));
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(SOLR_COLUMN_END).append(":[").append(params.getLong(HistorianServiceFields.FROM)).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());
        //TODO filter on names AND tags
        buildSolrFilterFromArray(params.getJsonArray(HistorianServiceFields.NAMES), SOLR_COLUMN_NAME)
                .ifPresent(query::addFilterQuery);
//            FIELDS_TO_FETCH
        query.setFields(SOLR_COLUMN_START,
                SOLR_COLUMN_END,
                SOLR_COLUMN_COUNT,
                SOLR_COLUMN_NAME);
        //    SORT
        query.setSort(SOLR_COLUMN_START, SolrQuery.ORDER.asc);
        query.addSort(SOLR_COLUMN_END, SolrQuery.ORDER.asc);
        query.setRows(params.getInteger(HistorianServiceFields.MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000));
        return query;
    }

    private Optional<String> buildSolrFilterFromArray(JsonArray jsonArray, String fieldToFilter) {
        if (jsonArray == null || jsonArray.isEmpty())
            return Optional.empty();
        if (jsonArray.size() == 1) {
            return Optional.of(fieldToFilter + ":\"" + jsonArray.getString(0) + "\"");
        } else {
            String orNames = jsonArray.stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining("\" OR \"", "(\"", "\")"));
            return Optional.of(fieldToFilter + ":" + orNames);
        }
    }
}
