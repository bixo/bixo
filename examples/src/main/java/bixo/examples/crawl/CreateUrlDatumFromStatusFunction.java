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
package bixo.examples.crawl;

import org.apache.log4j.Logger;

import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class CreateUrlDatumFromStatusFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateUrlDatumFromStatusFunction.class);

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

    // TODO VMa - verify w/Ken about this method...
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        StatusDatum datum = new StatusDatum(funcCall.getArguments());
        UrlStatus status = datum.getStatus();
        String url = datum.getUrl();
        long statusTime = datum.getStatusTime();
        long fetchTime;

        if (status == UrlStatus.FETCHED) {
            fetchTime = statusTime;
        } else if (status == UrlStatus.SKIPPED_BY_SCORER) {
            status = UrlStatus.FETCHED;
            fetchTime = statusTime; // Not strictly true, but we need old status
                                    // time passed through

            // TODO KKr - it would be nice to be able to get the old status
            // here, versus "knowing" that the only time a url is skipped by our
            // scorer is when it's already been fetched.
        } else if (status == UrlStatus.UNFETCHED) {
            // Since we only try to fetch URLs that have never been fetched, we
            // know that the last fetch time will always be 0.
            fetchTime = 0;
        } else {
            LOGGER.error(String.format("Unknown status %s for URL %s", status, url));
            return;
        }

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
