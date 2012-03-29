/*
 * Copyright 2009-2012 Scale Unlimited
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

import org.apache.log4j.Logger;

import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class CreateUrlDatumFromOutlinksFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateUrlDatumFromOutlinksFunction.class);

    public CreateUrlDatumFromOutlinksFunction() {
        super(UrlDatum.FIELDS);
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

            UrlDatum urlDatum = new UrlDatum(url);
            urlDatum.setPayload(datum.getPayload());
            collector.add(urlDatum.getTuple());
        }
    }
}
