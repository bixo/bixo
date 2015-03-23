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

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlNormalizer;
import bixo.urls.BaseUrlValidator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;


@SuppressWarnings({"serial", "rawtypes"})
public class CreateUrlDatumFromOutlinksFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateUrlDatumFromOutlinksFunction.class);
    private BaseUrlNormalizer _normalizer;
    private BaseUrlValidator _validator;

    public CreateUrlDatumFromOutlinksFunction(BaseUrlNormalizer normalizer, BaseUrlValidator urlValidator) {
        super(UrlDatum.FIELDS);
        _normalizer = normalizer;
        _validator = urlValidator;
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of outlink URLs");
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation of outlink URLs");
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        ParsedDatum datum = new ParsedDatum(funcCall.getArguments());
        Outlink outlinks[] = datum.getOutlinks();

        // Bump the crawl depth value only on a successful parse
        int crawlDepth = (Integer) datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);
        datum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, crawlDepth + 1);

        TupleEntryCollector collector = funcCall.getOutputCollector();

        for (Outlink outlink : outlinks) {
            String url = outlink.getToUrl();
            url = url.replaceAll("[\n\r]", "");
            url = _normalizer.normalize(url);
            if (_validator.isValid(url)) {
                UrlDatum urlDatum = new UrlDatum(url);
                urlDatum.setPayload(datum.getPayload());
                urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, UrlStatus.UNFETCHED.name());
                urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, Long.valueOf(0));
                collector.add(urlDatum.getTuple());
            }
        }
    }
}
