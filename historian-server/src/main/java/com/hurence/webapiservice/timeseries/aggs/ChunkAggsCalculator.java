package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.historian.mymodele.Chunk;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.OptionalDouble;

public class ChunkAggsCalculator extends AbstractAggsCalculator<Chunk> {

    public ChunkAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected double calculSum(List<Chunk> chunks) {
        return chunks.stream()
                .mapToDouble(Chunk::getSum)
                .sum();
    }

    @Override
    protected OptionalDouble calculMin(List<Chunk> chunks) {
        return chunks.stream()
                .mapToDouble(Chunk::getMin)
                .min();
    }

    @Override
    protected OptionalDouble calculMax(List<Chunk> chunks) {
        return chunks.stream()
                .mapToDouble(Chunk::getMax)
                .max();
    }

    @Override
    protected long getCount(List<Chunk> chunks) {
        return chunks.stream()
                .mapToLong(chunk -> chunk.getCount())
                .sum();
    }

}
