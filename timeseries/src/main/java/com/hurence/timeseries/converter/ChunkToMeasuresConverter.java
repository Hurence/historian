package com.hurence.timeseries.converter;

import com.hurence.timeseries.compaction.Compression;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesWithQualitySerializer;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.TreeSet;

public class ChunkToMeasuresConverter implements ChunkToMeasures {

    private static final Logger log = LoggerFactory.getLogger(ChunkToMeasuresConverter.class);

    @Override
    public TreeSet<Measure> buildMeasures(Chunk chunk)  {
        try (InputStream decompressed = Compression.decompressToStream(chunk.getValue())) {


            TreeSet<Measure> measures = ProtoBufTimeSeriesWithQualitySerializer.from(decompressed, chunk.getStart(), chunk.getEnd());
            measures.forEach(m -> {
                m.setName(chunk.getName());
                m.setTags(chunk.getTags());
            });
            return measures;
        }catch (IOException exception){
            log.error("something bad happened while building Measures from chunk: " + exception.toString());
            return new TreeSet<>();
        }
    }

}
