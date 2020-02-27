package com.hurence.historian;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.record.TimeSeriesRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.hurence.historian.App;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TimeseriesConverterTest {

    TimeseriesConverter converter = new TimeseriesConverter();

    String timeMs = "time_ms";
    String value = "value";
    String quality = "quality";
    String name = "name";

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
                createRow(new Object[]{1L, 1.0, 1, "metric_A"}),
                createRow(new Object[]{8L, 5.0, 1, "metric_A"}),
                createRow(new Object[]{4L, 2.0, 1, "metric_B"}),
                createRow(new Object[]{2L, 2.5, 1, "metric_A"})
        );
        StandardProcessContext context = new StandardProcessContext(converter, "");
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
//        assertEquals("my_metric",   timeseries.getTimeSeries());
    }

    private GenericRowWithSchema createRow(Object[] values) {
        return new GenericRowWithSchema(values, structType);
    }

}
