package bixo.operations;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.datum.UrlDatum;
import bixo.hadoop.ImportCounters;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import com.bixolabs.cascading.NullContext;

// TODO KKr - combine/resolve delta with UrlImporter
@SuppressWarnings("serial")
public class LoadUrlsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(LoadUrlsFunction.class);

    private int _maxUrls;
    private int _numUrls;

    public LoadUrlsFunction(int maxUrls) {
        super(UrlDatum.FIELDS);

        _maxUrls = maxUrls;
        _numUrls = 0;
    }

    public LoadUrlsFunction() {
        this(Integer.MAX_VALUE);
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        if (_numUrls >= _maxUrls) {
            process.increment(ImportCounters.URLS_FILTERED, 1);
            return;
        }
        
        String url = funcCall.getArguments().getString("line").trim();
        if ((url.length() == 0) || (url.startsWith("#"))) {
            return;
        }
        
        try {
            // Validate the URL
            new URL(url);
            
            UrlDatum urlDatum = new UrlDatum(url);
            funcCall.getOutputCollector().add(urlDatum.getTuple());
            
            _numUrls += 1;
            process.increment(ImportCounters.URLS_ACCEPTED, 1);
        } catch (MalformedURLException e) {
            LOGGER.warn("Invalid URL in input data file: " + url);
            process.increment(ImportCounters.URLS_REJECTED, 1);
        }
    }
}

