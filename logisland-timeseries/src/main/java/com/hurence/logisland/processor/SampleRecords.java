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


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.timeseries.sampling.record.RecordSampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@Tags({"analytic", "sampler", "record", "iot", "timeseries"})
@CapabilityDescription("Query matching based on `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>`_\n\n" +
        "you can use this processor to handle custom events defined by lucene queries\n" +
        "a new record is added to output each time a registered query is matched\n\n" +
        "A query is expressed as a lucene query against a field like for example: \n\n" +
        ".. code::\n" +
        "\n" +
        "   message:'bad exception'\n" +
        "   error_count:[10 TO *]\n" +
        "   bytes_out:5000\n" +
        "   user_name:tom*\n\n" +
        "Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations\n\n" +
        ".. warning::\n" +
        "   don't forget to set numeric fields property to handle correctly numeric ranges queries")
@ExtraDetailFile("./details/SampleRecords-Detail.rst")
public class SampleRecords extends AbstractProcessor {


    public static final PropertyDescriptor RECORD_VALUE_FIELD = new PropertyDescriptor.Builder()
            .name("record.value.field")
            .description("the name of the numeric field to sample")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_VALUE)
            .build();

    public static final PropertyDescriptor RECORD_TIME_FIELD = new PropertyDescriptor.Builder()
            .name("record.time.field")
            .description("the name of the time field to sample")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_TIME)
            .build();

    public static final AllowableValue NO_SAMPLING = new AllowableValue("none");
    public static final AllowableValue LTTB_SAMPLING = new AllowableValue("lttb");
    public static final AllowableValue AVERAGE_SAMPLING = new AllowableValue("average");
    public static final AllowableValue FIRST_ITEM_SAMPLING = new AllowableValue("first_item");
    public static final AllowableValue MIN_MAX_SAMPLING = new AllowableValue("min_max");
    public static final AllowableValue MODE_MEDIAN_SAMPLING = new AllowableValue("mode_median");


    public static final PropertyDescriptor SAMPLING_ALGORITHM = new PropertyDescriptor.Builder()
            .name("sampling.algorithm")
            .description("the implementation of the algorithm")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(NO_SAMPLING.getValue(), LTTB_SAMPLING.getValue(), AVERAGE_SAMPLING.getValue(), FIRST_ITEM_SAMPLING.getValue(), MIN_MAX_SAMPLING.getValue(), MODE_MEDIAN_SAMPLING.getValue())
            .build();

    public static final PropertyDescriptor SAMPLING_PARAMETER = new PropertyDescriptor.Builder()
            .name("sampling.parameter")
            .description("the parmater of the algorithm")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RECORD_VALUE_FIELD);
        descriptors.add(RECORD_TIME_FIELD);
        descriptors.add(SAMPLING_ALGORITHM);
        descriptors.add(SAMPLING_PARAMETER);

        return Collections.unmodifiableList(descriptors);
    }


    private static Logger logger = LoggerFactory.getLogger(SampleRecords.class);


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {


        SamplingAlgorithm algorithm = SamplingAlgorithm.valueOf(
                context.getPropertyValue(SAMPLING_ALGORITHM).asString().toUpperCase());
        String valueFieldName = context.getPropertyValue(RECORD_VALUE_FIELD).asString();
        String timeFieldName = context.getPropertyValue(RECORD_TIME_FIELD).asString();
        int parameter = context.getPropertyValue(SAMPLING_PARAMETER).asInteger();


        RecordSampler sampler = SamplerFactory.getRecordSampler(algorithm, valueFieldName, timeFieldName, parameter);

        return sampler.sample(new ArrayList<>(records)).stream()
                .map(r -> {
                    return r.setField("is_sampled", FieldType.BOOLEAN, true);
                }).collect(Collectors.toList());
    }


}
