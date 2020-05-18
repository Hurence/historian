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
package com.hurence.logisland.processor;


import com.hurence.logisland.record.*;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AddEvoaTagsTest {

    static String SAMPLED_RECORD = "sampled_record";

    private static Logger logger = LoggerFactory.getLogger(AddEvoaTagsTest.class);

    private Record createRecord(String name, long time, double value) {
        return new StandardRecord(SAMPLED_RECORD)
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, value)
                .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, time)
                .setField(TimeSeriesRecord.CHUNK_START, FieldType.LONG, time)
                .setField(FieldDictionary.RECORD_NAME, FieldType.STRING, name)
                .setField("name", FieldType.STRING, name);
    }





    private void printRecords(List<MockRecord> records) {
        records.forEach(r ->
                System.out.println(r.getField(FieldDictionary.RECORD_TIME).asLong() + ":" +
                        r.getField(FieldDictionary.RECORD_VALUE).asDouble())
        );

    }


    @Test
    public void validateMeta() {
        final String name = "T062.TC06_PV.F_CV";
        final TestRunner testRunner = TestRunners.newTestRunner(new AddEvoaTags());
        testRunner.assertValid();

        testRunner.clearQueues();
        testRunner.enqueue(createRecord(name,1581415021160L, 1.2d));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);



        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldEquals(TimeSeriesRecord.CHUNK_MONTH, 2);
        out.assertFieldEquals(TimeSeriesRecord.CHUNK_YEAR, 2020);
        out.assertFieldEquals(TimeSeriesRecord.CHUNK_DAY, 11);
        out.assertFieldEquals(TimeSeriesRecord.CHUNK_WEEK, 7);
        out.assertFieldEquals(TimeSeriesRecord.CODE_INSTALL, "T062");
        out.assertFieldEquals(TimeSeriesRecord.SENSOR, "TC06_PV");
        out.assertFieldEquals(TimeSeriesRecord.CHUNK_ORIGIN, "logisland");
    }

}
