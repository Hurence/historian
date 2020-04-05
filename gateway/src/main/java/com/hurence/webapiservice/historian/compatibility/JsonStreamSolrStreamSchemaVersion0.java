package com.hurence.webapiservice.historian.compatibility;

import com.hurence.webapiservice.historian.compatibility.JsonSchemaCompatibilityUtil;
import com.hurence.webapiservice.historian.impl.JsonStreamSolrStream;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

public class JsonStreamSolrStreamSchemaVersion0 extends JsonStreamSolrStream {


    public JsonStreamSolrStreamSchemaVersion0(TupleStream stream) {
        super(stream);
    }

    @Override
    protected JsonObject toJson(Tuple tuple) {
        final JsonObject json = super.toJson(tuple);
        return JsonSchemaCompatibilityUtil.convertSchema0ToCurrent(json);
    }
}
