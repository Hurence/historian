//package com.hurence.historian.solr.injector;
//
//import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
//import com.hurence.logisland.record.Point;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//public class EVOA0SolrInjectorDifferentMetricNames extends AbstractEVOA0SolrInjector {
//
//    private final int size;
//    private final int numberOfChunkByMetric;
//
//    public EVOA0SolrInjectorDifferentMetricNames(int numberOfMetric, int numberOfChunkByMetric) {
//        this.size = numberOfMetric;
//        this.numberOfChunkByMetric = numberOfChunkByMetric;
//    }
//
//    @Override
//    protected List<ChunkModeleVersion0> buildListOfChunks() {
//        List<ChunkModeleVersion0> chunks = IntStream.range(0, this.size)
//                .mapToObj(i -> "metric_" + i)
//                .map(this::buildChunkWithMetricName)
//                .flatMap(this::createMoreChunkForMetric)
//                .collect(Collectors.toList());
//        return chunks;
//    }
//
//    private ChunkModeleVersion0 buildChunkWithMetricName(String metricName) {
//        return ChunkModeleVersion0.fromPoints(metricName, Arrays.asList(
//                new Point(0, 1L, 5),
//                new Point(0, 2L, 8),
//                new Point(0, 3L, 1.2),
//                new Point(0, 4L, 6.5)
//        ));
//    }
//
//    private Stream<ChunkModeleVersion0> createMoreChunkForMetric(ChunkModeleVersion0 chunk) {
//        List<ChunkModeleVersion0> chunks = IntStream.range(0, this.numberOfChunkByMetric)
//                .mapToObj(i -> {
//                    //TODO eventually change chunk content if needed
//                    ChunkModeleVersion0 cloned = chunk;
//                    return cloned;
//                })
//                .collect(Collectors.toList());
//        return chunks.stream();
//    }
//}
