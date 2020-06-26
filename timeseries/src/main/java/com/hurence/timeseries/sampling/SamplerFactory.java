/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.timeseries.sampling;

import com.hurence.timeseries.modele.Point;

public class SamplerFactory {

    //TODO make this method generic
    //TODO Find a way to generate TimeSerieHandler with a factory, taking the class as param ?
    /**
     * Instanciates a sampler.
     *
     * @param algorithm the sampling algorithm
     * @param bucketSize an int parameter
     * @return the sampler
     */
    public static Sampler<Point> getPointSampler(SamplingAlgorithm algorithm, int bucketSize) {
        switch (algorithm) {
            case FIRST:
                return new FirstItemSampler<Point>(bucketSize);
            case AVERAGE:
                return new AverageSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case NONE:
                return new IsoSampler<Point>();
            case MIN:
                return new MinSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case MAX:
                return new MaxSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case MIN_MAX:
            case LTTB:
            case MODE_MEDIAN:
            default:
                throw new UnsupportedOperationException("algorithm " + algorithm.name() + " is not yet supported !");

        }
    }
    //TODO
    public static Sampler<Point> getOneTimePointSampler(SamplingAlgorithm algorithm, BucketingStrategy bucketingStrategy) {
        switch (algorithm) {
            case FIRST:
                return new FirstItemSamplerWithSpecificBucketing<Point>(bucketingStrategy);
            case AVERAGE:
//                return new AverageSampler<Point>(getPointTimeSerieHandler(), bucketingStrategy);
            case NONE:
//                return new IsoSampler<Point>();
            case MIN:
//                return new MinSampler<Point>(getPointTimeSerieHandler(), bucketingStrategy);
            case MAX:
//                return new MaxSampler<Point>(getPointTimeSerieHandler(), bucketingStrategy);
            case MIN_MAX:
            case LTTB:
            case MODE_MEDIAN:
            default:
                throw new UnsupportedOperationException("algorithm " + algorithm.name() + " is not yet supported !");

        }
    }

    public static TimeSerieHandler<Point> getPointTimeSerieHandler() {
        return new TimeSerieHandler<Point>() {
                @Override
                public Point createTimeserie(long timestamp, double value) {
                    return new Point(0, timestamp, value);
                }

                @Override
                public long getTimeserieTimestamp(Point point) {
                    return point.getTimestamp();
                }

                @Override
                public Double getTimeserieValue(Point point) {
                    return point.getValue();
                }
            };
    }
}
