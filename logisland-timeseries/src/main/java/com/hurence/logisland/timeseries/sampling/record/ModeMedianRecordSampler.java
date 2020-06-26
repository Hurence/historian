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
package com.hurence.logisland.timeseries.sampling.record;

import com.hurence.logisland.record.Record;

import java.util.List;

//TODO
public class ModeMedianRecordSampler extends AbstractRecordSampler {


    private int numBuckets;

    public ModeMedianRecordSampler(String valueFieldName, String timeFieldName, int numBuckets) {
        super(valueFieldName,timeFieldName);
        this.numBuckets = numBuckets;
    }


    /**
     * divide the points sequence into equally sized buckets
     * and select the first point of each bucket
     *
     * @param inputRecords the iput list
     * @return
     */
    @Override
    public List<Record> sample(List<Record> inputRecords) {
       return null;
    }
}
