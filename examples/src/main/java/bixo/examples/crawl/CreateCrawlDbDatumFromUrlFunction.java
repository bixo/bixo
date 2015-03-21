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

import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;


@SuppressWarnings({"serial", "rawtypes"})
public class CreateCrawlDbDatumFromUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCrawlDbDatumFromUrlFunction.class);

    private long _numCreated;

    public CreateCrawlDbDatumFromUrlFunction() {
        super(CrawlDbDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of crawldb datums");
        _numCreated = 0;
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation of crawldb datums");
        LOGGER.info("Crawldb datums created : " + _numCreated);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
        UrlDatum datum = new UrlDatum(funcCall.getArguments());
        Long lastFetched = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
        Long lastUpdated = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD);
        UrlStatus status = UrlStatus.valueOf((String) (datum.getPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD)));
        Integer crawlDepth = (Integer) datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);

        CrawlDbDatum crawldbDatum = new CrawlDbDatum(datum.getUrl(), lastFetched, lastUpdated, status, crawlDepth);

        funcCall.getOutputCollector().add(crawldbDatum.getTuple());
        _numCreated++;
    }
}
