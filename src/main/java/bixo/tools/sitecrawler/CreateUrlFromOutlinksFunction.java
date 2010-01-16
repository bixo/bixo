package bixo.tools.sitecrawler;

import java.util.Map;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class CreateUrlFromOutlinksFunction extends BaseOperation<NullContext> implements Function<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(CreateUrlFromOutlinksFunction.class);
	
    public CreateUrlFromOutlinksFunction() {
        super(UrlDatum.FIELDS.append(MetaData.FIELDS));
    }

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
		LOGGER.info("Starting creation of outlink URLs");
	}

	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
		LOGGER.info("Ending creation of outlink URLs");
	}

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
    	ParsedDatum datum = new ParsedDatum(funcCall.getArguments().getTuple(), MetaData.FIELDS);
    	Outlink outlinks[] = datum.getOutLinks();
    	
    	// Bump the crawl depth metadata value
    	Map<String, Comparable> metaData = datum.getMetaDataMap();
    	int crawlDepth = Integer.parseInt((String)metaData.get("crawl-depth"));
    	metaData.put("crawl-depth", Integer.toString(crawlDepth + 1));
    	
        TupleEntryCollector collector = funcCall.getOutputCollector();

    	for (Outlink outlink : outlinks) {
    	    String url = outlink.getToUrl();
    	    url = url.replaceAll("[\n\r]", "");
    	    
            UrlDatum urlDatum = new UrlDatum(url, 0, System.currentTimeMillis(), UrlStatus.UNFETCHED, metaData);
            collector.add(urlDatum.toTuple());
    	}
    }
}
