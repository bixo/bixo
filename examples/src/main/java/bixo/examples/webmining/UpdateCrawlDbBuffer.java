/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.examples.webmining;

import java.util.Iterator;

import bixo.datum.StatusDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;


@SuppressWarnings({"serial", "rawtypes"})
public class UpdateCrawlDbBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {

    private LoggingFlowProcess _flowProcess;

    private static final Fields CRAWLDBDATUM_URL_FIELD = new Fields(CrawlDbDatum.URL_FIELD);
    private static final Fields STATUSDATUM_URL_FIELD = new Fields(StatusDatum.URL_FN);
    private static final Fields ANALYZEDDATUM_URL_FIELD = new Fields(AnalyzedDatum.URL_FIELD);

    public UpdateCrawlDbBuffer() {
        super(CrawlDbDatum.FIELDS);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void prepare(FlowProcess flowProcess, 
                        OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
    }
    
    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.cleanup(flowProcess, operationCall);
        _flowProcess.dumpCounters();
    }
    
    @Override
    public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        
        // We will end up with 1- n entries of (C)rawlDbDatum, (S)tatusDatum, (A)nalyzedDatum, (L)inkDatum
        // [C | S | A | L] [C | S | A | L] [C | S | A | L] [C | S | A | L]

        CrawlDbDatum crawlDbDatum = null;
        StatusDatum statusDatum = null;
        AnalyzedDatum analyzedDatum = null;
        UrlStatus status = null;
        float pageScore = 0;
        float linkScore = 0;

        String url = null;

        while (iter.hasNext()) {
            TupleEntry entry = iter.next();
            
            boolean isCrawlDatum = entry.getString(CRAWLDBDATUM_URL_FIELD) != null;
            boolean isStatus = entry.getString(STATUSDATUM_URL_FIELD) != null;
            boolean isAnalyzed = entry.getString(ANALYZEDDATUM_URL_FIELD) != null;
            if (isCrawlDatum) {
               Tuple crawlDbTuple = TupleEntry.select(CrawlDbDatum.FIELDS, entry);
               crawlDbDatum = new CrawlDbDatum(crawlDbTuple);
               url = crawlDbDatum.getUrl();
            }
            
            if (isStatus) {
                statusDatum = new StatusDatum(entry);
                url = statusDatum.getUrl();
            }

            if (isAnalyzed) {
                Tuple analyzedTuple = TupleEntry.select(AnalyzedDatum.FIELDS, entry);
                analyzedDatum = new AnalyzedDatum(analyzedTuple);
                url = analyzedDatum.getUrl();
            }

            // we could have either status + link or just link tuple entry
            if (entry.getString(new Fields(LinkDatum.URL_FN)) != null) {
                LinkDatum linkDatum = new LinkDatum(TupleEntry.select(LinkDatum.FIELDS, entry));
                
                pageScore = linkDatum.getPageScore();
                // Add up the link scores
                linkScore += linkDatum.getLinkScore();
                url = linkDatum.getUrl();
            }
        }
        
        long lastFetched = 0;
        if (crawlDbDatum != null) {
            status = crawlDbDatum.getLastStatus();
            pageScore = crawlDbDatum.getPageScore();
            linkScore += crawlDbDatum.getLinksScore();
            lastFetched = crawlDbDatum.getLastFetched();
        } else if (statusDatum != null) {
            status = statusDatum.getStatus();
            if (status != UrlStatus.FETCHED ) {
                pageScore = 0; // if we didn't fetch the page, then we can't have a page score
                linkScore += (Float)statusDatum.getPayloadValue(CustomFields.LINKS_SCORE_FN);
            } else {
                if (analyzedDatum != null) {
                    pageScore = analyzedDatum.getPageScore();
                }
            }
            lastFetched = statusDatum.getStatusTime();
        } else {
            status = UrlStatus.UNFETCHED;
        }
            
        if (url == null) {
            throw new RuntimeException("There's a bug in this code - url shouldn't be null");
        }

        CrawlDbDatum updatedDatum = new CrawlDbDatum(url, lastFetched, status, pageScore, linkScore);
        bufferCall.getOutputCollector().add(updatedDatum.getTuple());
   }

}
