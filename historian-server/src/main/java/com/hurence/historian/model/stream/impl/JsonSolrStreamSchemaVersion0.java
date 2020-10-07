//package com.hurence.historian.modele.stream.impl;
//
//import io.vertx.core.json.JsonObject;
//import org.apache.solr.client.solrj.io.Tuple;
//
//public class JsonSolrStreamSchemaVersion0 extends JsonSolrStream {
//
//
//    public JsonSolrStreamSchemaVersion0(SolrStream stream) {
//        super(stream);
//    }
//
//    @Override
//    protected JsonObject toJson(Tuple tuple) {
//        final JsonObject json = super.toJson(tuple);
//        ChunkSchemaCompatibilityUtil.convertEVOA0ToInternalChunk(json);
//        return json;
//    }
//}
