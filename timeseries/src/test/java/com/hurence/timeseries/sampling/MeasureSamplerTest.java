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

import com.hurence.timeseries.model.Measure;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MeasureSamplerTest {

    private static final Logger logger = LoggerFactory.getLogger(MeasureSamplerTest.class);

    private List<Measure> getPoints() {
        return Arrays.asList(
                Measure.fromValue(1L, 48d),
                Measure.fromValue(2L, 52d),
                Measure.fromValue(3L, 60d)
        );
    }

    private List<Measure> getPoints2() {
        return Arrays.asList(
                Measure.fromValue(1L, 48d),
                Measure.fromValue(2L, 52d),
                Measure.fromValue(3L, 60d),
                Measure.fromValue(4L, 48d),
                Measure.fromValue(5L, 52d),
                Measure.fromValue(6L, 48d),
                Measure.fromValue(7L, 52d),
                Measure.fromValue(8L, 60d),
                Measure.fromValue(9L, 48d),
                Measure.fromValue(10L, 52d),
                Measure.fromValue(11L, 60d)
        );
    }

    @Test
    public void testAvgSampler() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE, 3);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(53.333333333333336d, measure1.getValue());
    }

    @Test
    public void bug27072020WhenBucketSuperiorToSizeInputDoesNotSample() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE, 1000);
        List<Measure> sampled = sampler.sample(getPoints2());
        Assertions.assertEquals(1, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(52.72727272727273d, measure1.getValue());
    }

    @Test
    public void testAvgSamplerNoFullBucket() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE,2);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(50d, measure1.getValue());
        Measure measure2 = sampled.get(1);
        Assertions.assertEquals(3L, measure2.getTimestamp());
        Assertions.assertEquals(60d, measure2.getValue());
    }

    @Test
    public void testFirstItemSampler() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST, 3);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(48d, measure1.getValue());
    }

    @Test
    public void testFirstItemSamplerNoFullBucket() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST,2);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(48d, measure1.getValue());
        Measure measure2 = sampled.get(1);
        Assertions.assertEquals(3L, measure2.getTimestamp());
        Assertions.assertEquals(60d, measure2.getValue());
    }

    @Test
    public void testNoneSampler() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE, 3);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(48d, measure1.getValue());
        Measure measure2 = sampled.get(1);
        Assertions.assertEquals(2L, measure2.getTimestamp());
        Assertions.assertEquals(52d, measure2.getValue());
        Measure measure3 = sampled.get(2);
        Assertions.assertEquals(3L, measure3.getTimestamp());
        Assertions.assertEquals(60d, measure3.getValue());
    }

    @Test
    public void testNoneSamplerNoFullBucket() {
        Sampler<Measure> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE,2);
        List<Measure> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        Measure measure1 = sampled.get(0);
        Assertions.assertEquals(1L, measure1.getTimestamp());
        Assertions.assertEquals(48d, measure1.getValue());
        Measure measure2 = sampled.get(1);
        Assertions.assertEquals(2L, measure2.getTimestamp());
        Assertions.assertEquals(52d, measure2.getValue());
        Measure measure3 = sampled.get(2);
        Assertions.assertEquals(3L, measure3.getTimestamp());
        Assertions.assertEquals(60d, measure3.getValue());
    }
}
