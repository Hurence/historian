package com.hurence.webapiservice.historian.handler;

import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import com.hurence.webapiservice.http.api.ingestion.JsonObjectToChunk;
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

import static com.hurence.historian.modele.HistorianFields.*;

public class AddTimeSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(AddTimeSeriesHandler.class);
    SolrHistorianConf solrHistorianConf;


    public AddTimeSeriesHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonObject>> getHandler(JsonObject timeseriesObject) {
        return p -> {
            try {
                final String chunkOrigin = timeseriesObject.getString(CHUNK_ORIGIN, "ingestion-json");
                JsonArray timeseriesPoints = timeseriesObject.getJsonArray(POINTS_REQUEST_FIELD);
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
                    int totalNumPointsInChunk = (int) document.getFieldValue(RESPONSE_CHUNK_COUNT_FIELD);
                    numChunk++;
                    numPoints = numPoints + totalNumPointsInChunk;
                }
                if(!documents.isEmpty()) {
                    LOGGER.info("adding some chunks in collection {}", solrHistorianConf.chunkCollection);
                    solrHistorianConf.client.add(solrHistorianConf.chunkCollection, documents);
                    solrHistorianConf.client.commit(solrHistorianConf.chunkCollection);
                    LOGGER.info("added with success some chunks in collection {}", solrHistorianConf.chunkCollection);
                }
                response.put(RESPONSE_TOTAL_ADDED_POINTS, numPoints).put(RESPONSE_TOTAL_ADDED_CHUNKS, numChunk);
                p.complete(response);
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                LOGGER.error("unexpected exception");
                p.fail(e);
            }
        };
    }

    private SolrInputDocument chunkTimeSerie(JsonObject timeserie, String chunkOrigin) {
        JsonObjectToChunk jsonObjectToChunk = new JsonObjectToChunk(chunkOrigin);
        SolrInputDocument doc = jsonObjectToChunk.chunkIntoSolrDocument(timeserie);
        return doc;
    }
}
