package com.hurence.historian;

import com.hurence.historian.processor.HistorianContext;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.timeseries.MetricTimeSeries;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TimeseriesConverterTest {

    TimeseriesConverter converter = new TimeseriesConverter();

    String timeMs = "time_ms";
    String value = "value";
    String quality = "quality";
    String name = "name";
    String code_install = "code_install";
    String month = "month";
    String year = "year";
    String sensor = "sensor";
    String day = "day";

    StructField[] structFields = new StructField[]{
            new StructField(timeMs, DataTypes.LongType, true, Metadata.empty()),
            new StructField(value, DataTypes.DoubleType, true, Metadata.empty()),
            new StructField(quality, DataTypes.DoubleType, true, Metadata.empty()),
            new StructField(name, DataTypes.StringType, true, Metadata.empty())
    };
    StructType structType = new StructType(structFields);

    @Test
    public void testparsingRequest() throws InitializationException {

        List<Row> rows = Arrays.asList(
                createRow(new Object[]{1L, 1.0, 1.0, "metric_A"}),
                createRow(new Object[]{8L, 5.0, 1.0, "metric_A"}),
                createRow(new Object[]{4L, 2.0, 1.1, "metric_B"}),
                createRow(new Object[]{2L, 2.5, 0.8, "metric_A"})
        );
        HistorianContext context = new HistorianContext(converter);
        context.setProperty(TimeseriesConverter.GROUPBY.getName(), "name");
        context.setProperty(TimeseriesConverter.METRIC.getName(),
                String.format("min;max;avg;trend;outlier;sax:%s,0.01,%s",
                        App.DEFAULT_SAX_ALPHABET_SIZE(), App.DEFAULT_SAX_STRING_LENGTH()));
        converter.init(context);
        TimeSeriesRecord timeseries = converter.fromRows("my_metric", rows);
        assertFalse(false);
        assertEquals("metric_A",   timeseries.getMetricName());
        assertEquals("timeseries",   timeseries.getType());
        assertEquals(4,   timeseries.getChunkSize());
        MetricTimeSeries metric = timeseries.getTimeSeries();
        assertEquals("my_metric",   metric.getName());
        assertEquals("timeseries",   metric.getType());
        assertEquals(8,   metric.getEnd());
        assertEquals(1,   metric.getStart());
        assertEquals(1,   metric.getTime(0));
        assertEquals(2,   metric.getTime(1));
        assertEquals(4,   metric.getTime(2));
        assertEquals(8,   metric.getTime(3));
        assertEquals(1.0,   metric.getValue(0));
        assertEquals(2.5,   metric.getValue(1));
        assertEquals(2.0,   metric.getValue(2));
        assertEquals(5.0,   metric.getValue(3));
        Map<String, Object> attribute = metric.getAttributesReference();
        assertEquals("",   attribute.get(code_install));
        assertEquals("",   attribute.get(month));
        assertEquals("",   attribute.get(year));
        assertEquals("metric_A",   attribute.get(name));
        assertEquals("",   attribute.get(sensor));
        assertEquals("",   attribute.get(day));
        assertEquals(1.0,   attribute.get(quality));
    }

    private GenericRowWithSchema createRow(Object[] values) {
        return new GenericRowWithSchema(values, structType);
    }

}
