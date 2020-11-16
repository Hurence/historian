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
package com.hurence.timeseries.sax;

import lombok.Builder;
import lombok.Data;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


@Data
@Builder
public class SaxConverter {

    private static Logger logger = LoggerFactory.getLogger(SaxConverter.class.getName());
    public static SAXProcessor saxProcessor = new SAXProcessor();
    public static NormalAlphabet normalAlphabet = new NormalAlphabet();


    @Builder.Default int paaSize = 7;
    @Builder.Default double nThreshold = 0;
    @Builder.Default int alphabetSize = 24;


    /**
     * convert a list of values into a SAX string
     * @param points
     * @return
     */
    public String run(List<Double> points) {

        double[] valuePoints = points.stream().mapToDouble(x -> x).toArray();
        try {

            char[] saxString = SaxConverter.saxProcessor
                    .ts2string(valuePoints,
                            paaSize,
                            SaxConverter.normalAlphabet.getCuts(alphabetSize), nThreshold);

            return new String(saxString);

        } catch (net.seninp.jmotif.sax.SAXException e) {
            logger.warn("Error while trying to calculate sax string for chunk", e.getMessage());
            return e.getMessage();
        }
    }

}
