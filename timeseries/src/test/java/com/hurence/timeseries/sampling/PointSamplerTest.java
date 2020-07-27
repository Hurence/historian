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

import com.hurence.timeseries.modele.PointImpl;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PointSamplerTest {

    private static final Logger logger = LoggerFactory.getLogger(PointSamplerTest.class);

    private List<PointImpl> getPoints() {
        return Arrays.asList(
                new PointImpl(1L, 48d),
                new PointImpl(2L, 52d),
                new PointImpl(3L, 60d)
        );
    }

    private List<PointImpl> getPoints2() {
        return Arrays.asList(
                new PointImpl(1L, 48d),
                new PointImpl(2L, 52d),
                new PointImpl(3L, 60d),
                new PointImpl(4L, 48d),
                new PointImpl(5L, 52d),
                new PointImpl(6L, 48d),
                new PointImpl(7L, 52d),
                new PointImpl(8L, 60d),
                new PointImpl(9L, 48d),
                new PointImpl(10L, 52d),
                new PointImpl(11L, 60d)
        );
    }

    @Test
    public void testAvgSampler() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE, 3);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(53.333333333333336d, point1.getValue());
    }

    @Test
    public void bug27072020WhenBucketSuperiorToSizeInputDoesNotSample() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE, 1000);
        List<PointImpl> sampled = sampler.sample(getPoints2());
        Assertions.assertEquals(1, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(52.72727272727273d, point1.getValue());
    }

    @Test
    public void testAvgSamplerNoFullBucket() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.AVERAGE,2);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(50d, point1.getValue());
        PointImpl point2 = sampled.get(1);
        Assertions.assertEquals(3L, point2.getTimestamp());
        Assertions.assertEquals(60d, point2.getValue());
    }

    @Test
    public void testFirstItemSampler() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST, 3);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(1, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
    }

    @Test
    public void testFirstItemSamplerNoFullBucket() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.FIRST,2);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(2, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        PointImpl point2 = sampled.get(1);
        Assertions.assertEquals(3L, point2.getTimestamp());
        Assertions.assertEquals(60d, point2.getValue());
    }

    @Test
    public void testNoneSampler() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE, 3);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        PointImpl point2 = sampled.get(1);
        Assertions.assertEquals(2L, point2.getTimestamp());
        Assertions.assertEquals(52d, point2.getValue());
        PointImpl point3 = sampled.get(2);
        Assertions.assertEquals(3L, point3.getTimestamp());
        Assertions.assertEquals(60d, point3.getValue());
    }

    @Test
    public void testNoneSamplerNoFullBucket() {
        Sampler<PointImpl> sampler = SamplerFactory.getPointSampler(SamplingAlgorithm.NONE,2);
        List<PointImpl> sampled = sampler.sample(getPoints());
        Assertions.assertEquals(3, sampled.size());
        PointImpl point1 = sampled.get(0);
        Assertions.assertEquals(1L, point1.getTimestamp());
        Assertions.assertEquals(48d, point1.getValue());
        PointImpl point2 = sampled.get(1);
        Assertions.assertEquals(2L, point2.getTimestamp());
        Assertions.assertEquals(52d, point2.getValue());
        PointImpl point3 = sampled.get(2);
        Assertions.assertEquals(3L, point3.getTimestamp());
        Assertions.assertEquals(60d, point3.getValue());
    }
}
