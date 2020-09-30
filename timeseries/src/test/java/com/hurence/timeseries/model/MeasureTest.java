package com.hurence.timeseries.model;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.converter.ChunkToMeasures;
import com.hurence.timeseries.converter.ChunkToMeasuresConverter;
import com.hurence.timeseries.converter.MeasuresToChunk;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public class MeasureTest {

  @Test
  public void testMeasure() {

    String measureName = "cpu";
    double measureValue = 76.4d;
    long measureTimestamp = 223849510000L;
    String measureLon = "5.373523";
    float measureQuality = 98.0f;
    String measureDay = "1977-02-03";
    Map<String,String> tags = new HashMap<>();
    tags.put("host" , "host1");
    tags.put("ip" ,"12.1.1.1");
    tags.put("lon" ,measureLon);

    Measure measure = Measure.builder()
      .name(measureName)
      .value(measureValue)
      .timestamp(measureTimestamp)
      .quality(measureQuality)
      .tags(tags)
      .build();

    assertEquals(measureName, measure.getName());
    assertEquals(measureValue, measure.getValue(), 0.0f);
    assertEquals(measureTimestamp, measure.getTimestamp());
    assertEquals(measureDay, measure.getDay());
    assertEquals(tags, measure.getTags());
    assertEquals(measureLon, measure.getTags().get("lon"));
    assertEquals(measureLon, measure.getTag("lon"));
    assertNull(measure.getTag("unknown tag"));
    assertTrue(measure.containsTag("host"));
  }
}
