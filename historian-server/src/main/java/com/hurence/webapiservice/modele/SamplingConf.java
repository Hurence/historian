package com.hurence.webapiservice.modele;

import com.hurence.timeseries.sampling.SamplingAlgorithm;

public class SamplingConf {
    private SamplingAlgorithm algo;
    private int bucketSize;
    private int maxPoint;

    public SamplingConf(SamplingAlgorithm algo, int bucketSize, int maxPoint) {
        this.algo = algo;
        this.bucketSize = bucketSize;
        this.maxPoint = maxPoint;
    }

    public SamplingAlgorithm getAlgo() {
        return algo;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public int getMaxPoint() {
        return maxPoint;
    }

    public void setAlgo(SamplingAlgorithm algo) {
        this.algo = algo;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public void setMaxPoint(int maxPoint) {
        this.maxPoint = maxPoint;
    }

    @Override
    public String toString() {
        return "SamplingConf{" +
                "algo=" + algo +
                ", bucketSize=" + bucketSize +
                ", maxPoint=" + maxPoint +
                '}';
    }
}
