package bixo.operations;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class LoadUrlsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(LoadUrlsFunction.class);
	
	public LoadUrlsFunction(Fields metadataFields) {
        super(UrlDatum.FIELDS.append(metadataFields));
	}

	@Override
	public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        String url = funcCall.getArguments().getString("line").trim();
        if ((url.length() == 0) || (url.startsWith("#"))) {
        	return;
        }
        
    	try {
        	// Validate the URL
            new URL(url);
            UrlDatum urlDatum = new UrlDatum(url);
            funcCall.getOutputCollector().add(urlDatum.toTuple());
        } catch (MalformedURLException e) {
            LOGGER.error("Invalid URL in input data file: " + url);
        }
	}
}

