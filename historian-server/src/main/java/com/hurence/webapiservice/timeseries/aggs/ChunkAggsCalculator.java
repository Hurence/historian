package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.OptionalDouble;

public class ChunkAggsCalculator extends AbstractAggsCalculator<ChunkVersionCurrent> {

    public ChunkAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected double calculSum(List<ChunkVersionCurrent> chunks) {
        return chunks.stream()
                .mapToDouble(ChunkVersionCurrent::getSum)
                .sum();
    }

    @Override
    protected OptionalDouble calculMin(List<ChunkVersionCurrent> chunks) {
        return chunks.stream()
                .mapToDouble(ChunkVersionCurrent::getMin)
                .min();
    }

    @Override
    protected OptionalDouble calculMax(List<ChunkVersionCurrent> chunks) {
        return chunks.stream()
                .mapToDouble(ChunkVersionCurrent::getMax)
                .max();
    }

    @Override
    protected long getCount(List<ChunkVersionCurrent> chunks) {
        return chunks.stream()
                .mapToLong(chunk -> chunk.getCount())
                .sum();
    }

}
