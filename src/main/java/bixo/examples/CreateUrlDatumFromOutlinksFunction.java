/*
 * Copyright (c) 2010 TransPac Software, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.examples;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

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
