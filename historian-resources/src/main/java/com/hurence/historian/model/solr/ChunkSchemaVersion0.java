package com.hurence.historian.model.solr;

import com.hurence.historian.model.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.model.SchemaVersion;

import java.util.Arrays;
import java.util.Collection;

public class ChunkSchemaVersion0 implements Schema {

    private static final Collection<SolrField> fields = Arrays.asList(
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.NAME)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.COMPACTIONS_RUNNING)
                    .withType("string")
                    .withMultivalued(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE)
                    .withType("string")
                    .withIndexed(false).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_START)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_END)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX)
                    .withType("ngramtext").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_OUTLIER)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_LAST)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_STDDEV)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY)
                    .withType("string").build()
    );

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    @Override
    public Collection<SolrField> getFields() {
        return fields;
    }
}
