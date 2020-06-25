package com.hurence.historian.modele;

import java.util.Arrays;
import java.util.Collection;

public class ReportSchemaVersion0 implements Schema {

    private static final Collection<Field> fields = Arrays.asList(
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.ID)
                    .withType("string")
                    .withRequired(true).build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.TYPE)
                    .withType("string")
                    .withRequired(true).build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.START)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.END)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.JOB_DURATION_IN_MILLI)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.STATUS)
                    .withType("string").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.NUMBER_OF_CHUNKS_IN_INPUT)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.NUMBER_OF_CHUNKS_IN_OUTPUT)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.TOTAL_METRICS_RECHUNKED)
                    .withType("plong").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.JOB_CONF)
                    .withType("string")
                    .withIndexed(false).build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.ERROR)
                    .withType("string").build(),
            new Field.Builder()
                    .withName(HistorianReportCollectionFieldsVersion0.EXCEPTION_MSG)
                    .withType("string").build()
    );

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    @Override
    public Collection<Field> getFields() {
        return fields;
    }
}
