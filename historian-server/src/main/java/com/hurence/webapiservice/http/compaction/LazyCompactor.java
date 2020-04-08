/*
 * Copyright (C) 2018 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.webapiservice.http.compaction;


import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.converter.common.DoubleList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import static com.hurence.webapiservice.http.compaction.ListUtils.sublist;
import static java.lang.Math.min;

/**
 * Takes documents and merges them to larger ones.
 *
 * @author alex.christ
 */
public class LazyCompactor {
    private final IndexSchema schema;
    private int ppc;

    /**
     * Creates an instance.
     *
     * @param pointsPerChunk the number of data points to be merged into a single document.
     *
     * @param schema    the current solr schema
     */
    public LazyCompactor(int pointsPerChunk, IndexSchema schema) {
        this.ppc = pointsPerChunk;
        this.schema = schema;
    }

    /**
     * Merges documents into larger ones
     *
     * @param documents the documents to compact
     * @return the compaction result
     */
    public Iterable<CompactionResult> compact(Iterable<Document> documents) {
        return new LazyCompactionResultSet(documents, schema);
    }

    private final class LazyCompactionResultSet implements Iterator<CompactionResult>, Iterable<CompactionResult> {
        private final Iterator<Document> documents;
        private final ConverterService converterService;
        private final IndexSchema schema;
        private LongList timestamps;
        private DoubleList values;
        private MetricTimeSeries currTs;

        private LazyCompactionResultSet(Iterable<Document> documents, IndexSchema schema) {
            this.documents = documents.iterator();
            this.schema = schema;
            this.converterService = new ConverterService();
            this.timestamps = new LongList();
            this.values = new DoubleList();
        }

        @Override
        public Iterator<CompactionResult> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return documents.hasNext();
        }

        @Override
        @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
        public CompactionResult next() {
            Set<Document> inputDocs = new HashSet<>();
            Set<SolrInputDocument> outputDocs = new HashSet<>();
            while (documents.hasNext()) {
                Document doc = documents.next();
                inputDocs.add(doc);

                currTs = converterService.toTimeSeries(doc, schema);
                timestamps.addAll(currTs.getTimestamps());
                values.addAll(currTs.getValues());

                if (timestamps.size() < ppc) {
                    continue;
                }

                int index = 0;
                while (index + ppc <= timestamps.size()) {
                    MetricTimeSeries slice = copyWithDataRange(currTs, index, index + ppc);
                    outputDocs.add(toSolrInputDocument(slice));
                    index += ppc;
                }

                // reduce timestamps and values to windows
                int start = min(index, timestamps.size());
                int end = timestamps.size();
                timestamps = sublist(timestamps, start, end);
                values = subList(values, start, end);

                break;
            }
            // write widows when all data points have been read
            if (!hasNext() && timestamps.size() > 0) {
                MetricTimeSeries slice = copyWithDataRange(currTs, 0, timestamps.size());
                outputDocs.add(converterService.toInputDocument(slice));
            }

            return new CompactionResult(inputDocs, outputDocs);
        }

        private DoubleList subList(DoubleList values, int start, int end) {
            return null;
        }

        /**
         * Calls {@link ConverterService#toInputDocument(MetricTimeSeries)} twice.
         * The second call should'nt be necessary since it seems to be side effect free.
         * Ff it's only called once, the resulting document sometimes contains wrong data.
         */
        private SolrInputDocument toSolrInputDocument(MetricTimeSeries slice) {
            @SuppressWarnings("UnusedAssignment")
            SolrInputDocument solrDoc = converterService.toInputDocument(slice);
            solrDoc = converterService.toInputDocument(slice);
            return solrDoc;
        }

        private MetricTimeSeries copyWithDataRange(MetricTimeSeries ts, int start, int end) {
            return converterService.copy(ts)
                    .points(sublist(timestamps, start, end),
                            subList(values, start, end))
                    .start(timestamps.get(start))
                    .end(timestamps.get(end - 1))
                    .build();
        }
    }
}