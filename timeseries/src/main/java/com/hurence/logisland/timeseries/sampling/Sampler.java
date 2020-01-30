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
package com.hurence.logisland.timeseries.sampling;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Sampler<ELEMENT> {


    /**
     * Reduce the number of inputs elements accordingly to a sampling strategy
     *
     * @param toBeSampled the given elements to sample
     * @return the sampled elements
     */
    List<ELEMENT> sample(List<ELEMENT> toBeSampled);

    /**
     * Reduce the number of inputs elements accordingly to a sampling strategy
     *
     * @param toBeSampled the given elements to sample
     * @return the sampled elements
     */
    default Stream<ELEMENT> sample(Stream<ELEMENT> toBeSampled) {
        List<ELEMENT> toBeSampledList = toBeSampled.collect(Collectors.toList());
        return sample(toBeSampledList).stream();
    }
}
