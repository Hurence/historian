package com.hurence.historian.modele.solr;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion1;
import com.hurence.historian.modele.SchemaVersion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class ChunkSchemaVersion1 extends ChunkSchemaVersion0 implements Schema{

    private Collection<SolrField> fields = new ArrayList<>(super.getFields());

    public ChunkSchemaVersion1(){ // TODO check this correct or not !
        fields.addAll(Arrays.asList(
                new SolrField.Builder()
                        .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_AVG)
                        .withType("pfloat").build(),
                new SolrField.Builder()
                        .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MIN)
                        .withType("pfloat").build(),
                new SolrField.Builder()
                        .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MAX)
                        .withType("pfloat").build(),
                new SolrField.Builder()
                        .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_SUM)
                        .withType("pfloat").build(),
                new SolrField.Builder()
                        .withName(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_FIRST)
                        .withType("pfloat").build()
        ));
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }

    @Override
    public Collection<SolrField> getFields() {
        return fields;
    }
}
