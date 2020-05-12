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
import com.hurence.logisland.record.EvoaUtils;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.TimeSeriesRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
            EvoaUtils.setDateFields(record);
            EvoaUtils.setBusinessFields(record);
            EvoaUtils.setChunkOrigin(record, TimeSeriesRecord.CHUNK_ORIGIN_LOGISLAND);
            EvoaUtils.setHashId(record);
        });

        return records;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }
}

