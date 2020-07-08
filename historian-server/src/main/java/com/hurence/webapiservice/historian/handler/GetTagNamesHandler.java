package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.solr.SolrField;
import com.hurence.historian.modele.solr.Schema;
import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.NAME;

public class GetTagNamesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTagNamesHandler.class);
    private static Set<String> fieldsThatAreNotTags = new HashSet<>(Arrays.asList(
            "_nest_path_","_root_","_text_","_version_"
    ));


    SolrHistorianConf solrHistorianConf;


    public GetTagNamesHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    public Handler<Promise<JsonArray>> getHandler() {

        SchemaRequest request = new SchemaRequest();
        return p -> {
            try {
                SchemaResponse response = request.process(solrHistorianConf.client, solrHistorianConf.chunkCollection);
                List<String> tags = response.getSchemaRepresentation().getFields().stream()
                        .map(fieldMap -> (String) fieldMap.get(NAME)).collect(Collectors.toList());
                Collection<String> schemaFields = Schema.getChunkSchema(solrHistorianConf.schemaVersion).getFields()
                        .stream().map(SolrField::getName)
                        .collect(Collectors.toList());
                tags.removeAll(schemaFields);
                tags.removeAll(fieldsThatAreNotTags);
                if (tags.size() == 0) {
                    p.complete(new JsonArray()
                    );
                    return;
                }
                LOGGER.debug("Found " + tags.size() + " different tags");
                p.complete(new JsonArray(tags)
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
