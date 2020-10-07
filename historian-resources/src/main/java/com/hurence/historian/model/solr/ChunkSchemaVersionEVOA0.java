package com.hurence.historian.model.solr;

import com.hurence.historian.model.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.historian.model.SchemaVersion;

import java.util.Arrays;
import java.util.Collection;

/**
 * Base on snapshot EVOA0
 */
public class ChunkSchemaVersionEVOA0 implements Schema {

    private static final Collection<SolrField> fields = Arrays.asList(
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.NAME)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.TAGS)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.QUALITY)
                    .withType("pfloat").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE)
                    .withType("string")
                    .withIndexed(false).build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_WINDOW_MS)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CODE_INSTALL)
                    .withType("string").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.TIMESTAMP)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END)
                    .withType("plong").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE_BYTES)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_COUNT)
                    .withType("pint").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MIN)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MAX)
                    .withType("pdouble").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SAX)
                    .withType("ngramtext").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND)
                    .withType("boolean").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_ORIGIN)
                    .withType("text_general").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.DELETE)
                    .withType("text_general").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.NUMERIC_TYPE)
                    .withType("text_general").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.FILE_PATH)
                    .withType("text_general").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_OUTLIER)
                    .withType("booleans").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST)
                    .withType("pdoubles").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM)
                    .withType("pdoubles").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.VALUE)
                    .withType("pdoubles").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR)
                    .withType("plongs").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH)
                    .withType("plongs").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY)
                    .withType("plongs").build(),
            new SolrField.Builder()
                    .withName(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_WEEK)
                    .withType("plongs").build()
    );

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }

    @Override
    public Collection<SolrField> getFields() {
        return fields;
    }
}
