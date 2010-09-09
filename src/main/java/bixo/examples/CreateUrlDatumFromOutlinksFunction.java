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
