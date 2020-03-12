package com.hurence.historian.solr.injector;

import com.hurence.historian.modele.ChunkModele;
import com.hurence.logisland.record.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GeneralSolrInjector extends AbstractSolrInjector {

    private List<ChunkModele> chunks = new ArrayList<>();

    @Override
    protected List<ChunkModele> buildListOfChunks() {
        return chunks;
    }

    public void addChunk(String metric, int year, int month, int day, String origin, List<Point> points) {
        ChunkModele chunk = ChunkModele.fromPoints(metric, year, month, day, origin, points);
        chunks.add(chunk);
    }

    public void addChunk(String metric, String origin, List<Point> points) {
        ChunkModele chunk = ChunkModele.fromPoints(metric, 1, 1, 1, origin, points);
        chunks.add(chunk);
    }
}