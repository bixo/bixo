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
package bixo.examples.crawl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;


@SuppressWarnings({"serial", "rawtypes"})
public class CreateUrlDatumFromStatusFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateUrlDatumFromStatusFunction.class);

    private int _numCreated;

    public CreateUrlDatumFromStatusFunction() {
        super(UrlDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of URLs from status");
        _numCreated = 0;
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation of URLs from status");
        LOGGER.info(String.format("Created %d URLs", _numCreated));
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        StatusDatum datum = new StatusDatum(funcCall.getArguments());
        UrlStatus status = datum.getStatus();
        String url = datum.getUrl();
        long statusTime = datum.getStatusTime();
        
        long fetchTime = statusTime; // Not exactly true... since in some cases we
                    // may not have fetched the url. But because we are sharing this logic
                    // between the JDBCCrawlTool and the DemoCrawlTool, we use the value
                    // of the fetchTime while selecting the "latest" url. Newly added urls
                    // have a fetchTime of 0, so in order to preserve say a SKIPPED status
                    // we set the fetch time here.

        _numCreated += 1;

        UrlDatum urlDatum = new UrlDatum(url);
        urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, fetchTime);
        urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, statusTime);
        urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, status.name());
        // Don't change the crawl depth here - we do that only in the case of a
        // successful parse
        urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH));

        funcCall.getOutputCollector().add(urlDatum.getTuple());
    }
}
