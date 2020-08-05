package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianConf;
import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.SolrHistorianConf;
import com.hurence.webapiservice.http.api.ingestion.JsonObjectToChunkVersion0;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class AddTimeSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(AddTimeSeriesHandler.class);
    HistorianConf historianConf;
    SolrHistorianConf solrHistorianConf;

    public AddTimeSeriesHandler(HistorianConf historianConf, SolrHistorianConf solrHistorianConf) {
        this.historianConf = historianConf;
        this.solrHistorianConf = solrHistorianConf;
    }

    private SolrFieldMapping getHistorianFields() {
        return this.historianConf.getFieldsInSolr();
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject timeseriesObject) {
        return p -> {
            try {
                final String chunkOrigin = timeseriesObject.getString(HistorianServiceFields.ORIGIN, "ingestion-json");
                JsonArray timeseriesPoints = timeseriesObject.getJsonArray(HistorianServiceFields.POINTS);
                JsonObject response = new JsonObject();
                Collection<SolrInputDocument> documents = new ArrayList<>();
                int numChunk = 0;
                int numPoints = 0;
                for (Object timeserieObject : timeseriesPoints) {
                    JsonObject timeserie = (JsonObject) timeserieObject;
                    SolrInputDocument document;
                    LOGGER.info("building SolrDocument from a chunk");
                    document = chunkTimeSerie(timeserie, chunkOrigin);
                    documents.add(document);
                    int totalNumPointsInChunk = (int) document.getFieldValue(getHistorianFields().CHUNK_COUNT_FIELD);
                    numChunk++;
                    numPoints = numPoints + totalNumPointsInChunk;
                }
                if(!documents.isEmpty()) {
                    LOGGER.info("adding some chunks in collection {}", solrHistorianConf.chunkCollection);
                    solrHistorianConf.client.add(solrHistorianConf.chunkCollection, documents);
                    solrHistorianConf.client.commit(solrHistorianConf.chunkCollection);
                    LOGGER.info("added with success some chunks in collection {}", solrHistorianConf.chunkCollection);
                }
                response.put(HistorianServiceFields.TOTAL_ADDED_POINTS, numPoints).put(HistorianServiceFields.TOTAL_ADDED_CHUNKS, numChunk);
                p.complete(response);
            } catch (SolrServerException | IOException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }

    private SolrInputDocument chunkTimeSerie(JsonObject timeserie, String chunkOrigin) {
        //Only version 0 is currently supporting creation from rest api
        JsonObjectToChunkVersion0 jsonObjectToChunkVersion0 = new JsonObjectToChunkVersion0(chunkOrigin);
        SolrInputDocument doc = jsonObjectToChunkVersion0.chunkIntoSolrDocument(timeserie);
        return doc;
    }
}
