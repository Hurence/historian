package com.hurence.historian.converter;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Definitions;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.stream.Collectors;

import static com.hurence.timeseries.model.Definitions.*;

public class SolrDocumentReader {
    private static Logger LOGGER = LoggerFactory.getLogger(SolrDocumentReader.class);

    public static Chunk fromSolrDocument(SolrDocument solrDocument) {

        Chunk.ChunkBuilder builder = Chunk.builder();

        Class builderClass = builder.getClass();

        HashMap<String, String> tags = new HashMap<String, String>();

        for (String field : solrDocument.getFieldNames().stream().sorted().collect(Collectors.toList())) {

            if (SOLR_COLUMNS.contains(field)) { // i.e: chunk_day
                Object value = solrDocument.get(field);
                if (value == null) {
                    continue;
                }

                if (field.equals(SOLR_COLUMN_VALUE)) {
                    builder.value(BinaryEncodingUtils.decode((String) value));
                } else if (field.equals(SOLR_COLUMN_VERSION)) {
                    builder.version(SchemaVersion.valueOf((String) value));
                } else if (field.equals(SOLR_COLUMN_COUNT)) {
                    Long longValue = Long.valueOf(value.toString());
                    builder.count(longValue);
                } else {
                    // For other fields, use reflexion
                    String chunkField = Definitions.getFieldFromColumn(field); // i.e: chunk_day -> day
                    Method method = null;
                    Class valueClass = value.getClass();
                    if (valueClass.equals(Long.class)) {
                        valueClass = long.class;
                    }
                    if (valueClass.equals(Integer.class)) {
                        valueClass = int.class;
                    }
                    if (valueClass.equals(Double.class)) {
                        valueClass = double.class;
                    }
                    if (valueClass.equals(Float.class)) {
                        valueClass = float.class;
                    }
                    if (valueClass.equals(Boolean.class)) {
                        valueClass = boolean.class;
                    }
                    try {
                        method = builderClass.getMethod(chunkField, valueClass); // i.e builder.day(value)
                    } catch (NoSuchMethodException e) {
                        LOGGER.error("error looking for method " + chunkField, e);
                    }
                    try {
                        method.invoke(builder, value);
                    } catch (Exception e) {
                        LOGGER.error("error calling while method " + method, e);
                    }
                }
            } else if (field.equals("_version_")) {
                continue;
            } else {
                // This is a tag field
                tags.put(field, (String)solrDocument.get(field));
            }
        }

        builder.tags(tags);
        return builder.buildId().build();
    }
}
