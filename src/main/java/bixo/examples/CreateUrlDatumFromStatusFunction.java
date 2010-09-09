package bixo.examples;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

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
            // here,
            // versus "knowing" that the only time a url is skipped by our
            // scorer is
            // when it's already been fetched.
        } else if (status == UrlStatus.UNFETCHED) {
            // Since we only try to fetch URLs that have never been fetched, we
            // know that the
            // last fetch time will always be 0.
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
