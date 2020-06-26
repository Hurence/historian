package com.hurence.historian;

import com.hurence.historian.processor.HistorianContext;
import com.hurence.historian.processor.HistorianProcessor;
import com.hurence.logisland.BinaryCompactionConverterOfRecord;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.serializer.KryoSerializer;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.functions.*;
import com.hurence.logisland.timeseries.metric.MetricType;
import com.hurence.logisland.timeseries.query.QueryEvaluator;
import com.hurence.logisland.timeseries.query.TypeFunctions;
import com.hurence.logisland.util.string.BinaryEncodingUtils;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TimeseriesConverter implements HistorianProcessor {

    public static final PropertyDescriptor GROUPBY = new PropertyDescriptor.Builder()
            .name("groupby")
            .description("The field the chunk should be grouped by")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("name")
            .build();

    public static final PropertyDescriptor METRIC = new PropertyDescriptor.Builder()
            .name("metric")
            .description("The chronix metric to calculate for the chunk")
            .required(false)
            .addValidator(StandardValidators.SEMICOLON_SEPARATED_LIST_VALIDATOR)
            .build();

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUPBY);
        descriptors.add(METRIC);
        return descriptors;
    }

    private Logger logger = LoggerFactory.getLogger(TimeseriesConverter.class);

    private List<ChronixTransformation> transformations = Collections.emptyList();
    private List<ChronixAggregation> aggregations = Collections.emptyList();
    private List<ChronixAnalysis> analyses = Collections.emptyList();
    private List<ChronixEncoding> encodings = Collections.emptyList();
    private FunctionValueMap functionValueMap = new FunctionValueMap(0, 0, 0, 0);

    private BinaryCompactionConverterOfRecord binaryCompactor;
    private List<String> groupBy;
    private final KryoSerializer serializer = new KryoSerializer(true);


    public void init(HistorianContext context) throws InitializationException {

        // init binary converter
        final String[] groupByArray = context.getPropertyValue(GROUPBY).asString().split(",");
        groupBy = Arrays.stream(groupByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        BinaryCompactionConverterOfRecord.Builder builder = new BinaryCompactionConverterOfRecord.Builder();
        binaryCompactor = builder.build();

        // init metric functions
        if (context.getPropertyValue(METRIC).isSet()) {
            String[] metric = {"metric{" + context.getPropertyValue(METRIC).asString() + "}"};

            TypeFunctions functions = QueryEvaluator.extractFunctions(metric);

            analyses = functions.getTypeFunctions(new MetricType()).getAnalyses();
            aggregations = functions.getTypeFunctions(new MetricType()).getAggregations();
            transformations = functions.getTypeFunctions(new MetricType()).getTransformations();
            encodings = functions.getTypeFunctions(new MetricType()).getEncodings();
            functionValueMap = new FunctionValueMap(aggregations.size(), analyses.size(), transformations.size(), encodings.size());
        }
    }


    /**
     * gets the kryo bytes representation of a ts record
     *
     * @param tsRecord
     * @return
     */
    public byte[] serialize(TimeSeriesRecord tsRecord) {

        // has Id
        final String hashString = DigestUtils.sha256Hex(tsRecord.getField(TimeSeriesRecord.CHUNK_VALUE).asBytes());
        tsRecord.setId(hashString);

        // encode chunk_value to base64
        Field f = tsRecord.getField(TimeSeriesRecord.CHUNK_VALUE);
        if (f != null) {
            if (!(f.getType() == FieldType.BYTES || f.getType() == FieldType.NULL)) {
                logger.error("Field type '{}' is not an array of bytes",
                        new Object[]{f.getName()});
            } else {
                byte[] content = f.asBytes();
                if (content != null) {
                    try {
                        tsRecord.setStringField(TimeSeriesRecord.CHUNK_VALUE, BinaryEncodingUtils.encode(content));
                    } catch (Exception e) {
                        logger.error("Unable to encode field '{}' : {}",
                                new Object[]{f.getName(), e.getMessage()});
                    }
                }
            }
        }

        // get thoses bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(baos, tsRecord);
        try {
            baos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            logger.error(
                    "Unable to serialize field record : {}",
                    new Object[]{e.getMessage()});
            return null;
        }
    }

    public TimeSeriesRecord computeValue(TimeSeriesRecord tsRecord) {

        try{
            byte[] bytes = binaryCompactor.serializeTimeseries(tsRecord.getTimeSeries());
            String chunkValueBase64 = BinaryEncodingUtils.encode(bytes);
            tsRecord.setStringField(TimeSeriesRecord.CHUNK_VALUE, chunkValueBase64);
            tsRecord.setIntField(TimeSeriesRecord.CHUNK_SIZE_BYTES, bytes.length);
        } catch (Exception ex) {
            logger.error(
                    "Unable to convert chunk_vlaue to base64 : {}",
                    new Object[]{ex.getMessage()});
        }

        return tsRecord;
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    public TimeSeriesRecord computeMetrics(TimeSeriesRecord tsRecord) {

        MetricTimeSeries timeSeries = tsRecord.getTimeSeries();
        functionValueMap.resetValues();

        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
            String name = functionValueMap.getAggregation(i).getQueryName();
            double value = functionValueMap.getAggregationValue(i);
            tsRecord.setField("chunk_" + name, FieldType.DOUBLE, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            tsRecord.setField("chunk_" + name, FieldType.BOOLEAN, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            tsRecord.setField("chunk_" + name, FieldType.STRING, value);
        }


        return tsRecord;
    }

    /**
     * Converts a list of records to a timeseries chunk
     *
     * @return
     */
    public TimeSeriesRecord fromRecords(List<Record> groupedRecords) {
        TimeSeriesRecord tsRecord = binaryCompactor.chunk(groupedRecords);
        return computeMetrics(tsRecord);
    }

    /**
     * Converts a list of rows to a timeseries chunk
     *
     * @param metricName
     * @param rows
     * @return
     */
    public TimeSeriesRecord fromRows(String metricName, List<Row> rows) {

        // Convert first to logisland records
        List<Record> groupedRecords = new ArrayList<>();
        for (Row r : rows) {
            try {
                long time = r.getLong(r.fieldIndex("time_ms"));

                double value = Double.MAX_VALUE;
                try {
                    value = r.getDouble(r.fieldIndex("value"));
                } catch (Exception e) {
                    try {
                        value = Double.parseDouble(r.getString(r.fieldIndex("value")));
                    } catch (Exception e2) {
                        logger.error("unable to parse value for row : " + r.toString());
                    }

                }

                double quality = Double.MAX_VALUE;
                try {
                    quality = r.getDouble(r.fieldIndex("quality"));
                } catch (Exception e) {
                    try {
                        quality = Double.parseDouble(r.getString(r.fieldIndex("quality")));
                    } catch (Exception e2) {
                        logger.error("unable to parse quality for row : " + r.toString());
                    }
                }

                String name = r.getString(r.fieldIndex("name"));
                String codeInstall = "";
                try {
                    codeInstall = r.getString(r.fieldIndex("code_install"));
                } catch (Exception ex) {
                    //do nothing
                }
                String sensor = "";
                try {
                    sensor = r.getString(r.fieldIndex("sensor"));
                } catch (Exception ex) {
                    //do nothing
                }

                String year = "";
                try {
                    year = String.valueOf(r.getInt(r.fieldIndex("year")));
                } catch (Exception ex) {
                    //do nothing
                }
                String month = "";
                try {
                    month = String.valueOf(r.getInt(r.fieldIndex("month")));
                } catch (Exception ex) {
                    //do nothing
                }
                String day = "";
                try {
                    day = String.valueOf(r.getInt(r.fieldIndex("day")));
                } catch (Exception ex) {
                    //do nothing
                }

                Record record = new StandardRecord(RecordDictionary.TIMESERIES)
                        .setStringField(FieldDictionary.RECORD_NAME, metricName)
                        .setDoubleField(FieldDictionary.RECORD_VALUE, value)
                        .setDoubleField("quality", quality)
                        .setStringField("name", name)
                        .setStringField("year", year)
                        .setStringField("month", month)
                        .setStringField("day", day)
                        .setStringField("code_install", codeInstall)
                        .setStringField("sensor", sensor)
                        .setTime(time);

                groupedRecords.add(record);
            } catch (Exception e) {
                logger.error("unable to parse row : " + r.toString());
            }

        }

        return fromRecords(groupedRecords);
    }

    /**
     * Converts a list of measures to a timeseries chunk
     *
     * @param measures
     * @return
     */
    public TimeSeriesRecord fromMeasurestoTimeseriesRecord(List<EvoaMeasure> measures) {

        // Convert first to logisland records
        List<Record> groupedRecords = new ArrayList<>();
        for (EvoaMeasure measure : measures) {
            try {
                Record record = new StandardRecord(RecordDictionary.TIMESERIES)
                        .setStringField(FieldDictionary.RECORD_NAME, measure.name())
                        .setDoubleField(FieldDictionary.RECORD_VALUE, measure.value())
                        .setDoubleField("quality", measure.quality())
                        .setStringField("name", measure.name())
                        .setIntField("year", measure.year())
                        .setIntField("month", measure.month())
                        .setIntField("week", measure.week())
                        .setIntField("day", measure.day())
                        .setStringField("code_install", measure.codeInstall())
                        .setStringField("sensor", measure.sensor())
                        .setStringField("file_path", measure.filePath())
                        .setTime(measure.timeMs());

                groupedRecords.add(record);
            } catch (Exception e) {
                logger.error("unable to parse row : " + measure.toString());
            }

        }
        TimeSeriesRecord tsRecord = binaryCompactor.chunk(groupedRecords);
        return computeMetricsTimeseriesRecord(tsRecord);
    }

    private TimeSeriesRecord computeMetricsTimeseriesRecord(TimeSeriesRecord tsRecord) {
        MetricTimeSeries timeSeries = tsRecord.getTimeSeries();

        functionValueMap.resetValues();

        transformations.forEach(transfo -> transfo.execute(timeSeries, functionValueMap));
        analyses.forEach(analyse -> analyse.execute(timeSeries, functionValueMap));
        aggregations.forEach(aggregation -> aggregation.execute(timeSeries, functionValueMap));
        encodings.forEach(encoding -> encoding.execute(timeSeries, functionValueMap));

        for (int i = 0; i < functionValueMap.sizeOfAggregations(); i++) {
            String name = functionValueMap.getAggregation(i).getQueryName();
            double value = functionValueMap.getAggregationValue(i);
            tsRecord.setField("chunk_" + name, FieldType.DOUBLE, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfAnalyses(); i++) {
            String name = functionValueMap.getAnalysis(i).getQueryName();
            boolean value = functionValueMap.getAnalysisValue(i);
            tsRecord.setField("chunk_" + name, FieldType.BOOLEAN, value);
        }

        for (int i = 0; i < functionValueMap.sizeOfEncodings(); i++) {
            String name = functionValueMap.getEncoding(i).getQueryName();
            String value = functionValueMap.getEncodingValue(i);
            tsRecord.setField("chunk_" + name, FieldType.STRING, value);
        }


        return tsRecord;
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {

        for (PropertyDescriptor p : getSupportedPropertyDescriptors()) {


            if (p.getName().equals(name))
                return p;
        }

        return null;


    }
}
