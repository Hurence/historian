/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.TimeSeriesRecord;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"record", "fields", "timeseries", "chronix", "evoa"})
@CapabilityDescription("Some IFP specifics")
public class AddEvoaTags extends AbstractProcessor {


    Pattern csvRegexp = Pattern.compile("(\\w+)\\.?(\\w+-?\\w+-?\\w+)?\\.?(\\w+)?");


    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);

    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        records.forEach(record -> {

            /**
             * set some data metas
             */
            if (record.hasField(TimeSeriesRecord.CHUNK_START)) {
                try {
                    Instant instant = Instant.ofEpochMilli(record.getField(TimeSeriesRecord.CHUNK_START).asLong());
                    LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
                    int month = localDate.getMonthValue();
                    int day = localDate.getDayOfMonth();
                    int year = localDate.getYear();
                    int week = localDate.get( WeekFields.ISO.weekOfWeekBasedYear() ) ;

                    record.setIntField("month", month);
                    record.setIntField("day", day);
                    record.setIntField("year", year);
                    record.setIntField("week", week);
                } catch (Exception ex) {
                    //do nothing
                }
            }

            /**
             * set some specific fields
             */
            if (record.hasField("name")) {

                try {


                    Matcher m = csvRegexp.matcher(record.getField("name").asString());
// lancement de la recherche de toutes les occurrences
                    boolean b = m.matches();
// si recherche fructueuse
                    if (b) {
                        record.setStringField("code_install", m.group(1));
                        record.setStringField("sensor", m.group(2));
                    }


                    record.setStringField(TimeSeriesRecord.CHUNK_ORIGIN, "logisland");
                } catch (Exception ex) {
                    //do nothing
                }
            }


        });


        return records;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }
}

