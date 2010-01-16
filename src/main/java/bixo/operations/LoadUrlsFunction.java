package bixo.operations;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.BaseDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.hadoop.ImportCounters;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;

// TODO KKr - combine/resolve delta with UrlImporter
@SuppressWarnings("serial")
public class LoadUrlsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(LoadUrlsFunction.class);

    private int _maxUrls;
    private int _numUrls;

    public LoadUrlsFunction(int maxUrls, Fields metadataFields) {
        super(UrlDatum.FIELDS.append(metadataFields));

        _maxUrls = maxUrls;
        _numUrls = 0;
    }

    public LoadUrlsFunction(Fields metadataFields) {
        this(Integer.MAX_VALUE, metadataFields);
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
        
        UrlStatus status;
        
        try {
            // Validate the URL
            new URL(url);
            status = UrlStatus.UNFETCHED;
        } catch (MalformedURLException e) {
            LOGGER.warn("Invalid URL in input data file: " + url);
            status = UrlStatus.SKIPPED_INVALID_URL;
            process.increment(ImportCounters.URLS_REJECTED, 1);
        }
        
        // TODO KKr - do we need to worry about creating a map from the metadata fields?
        UrlDatum urlDatum = new UrlDatum(url, 0, System.currentTimeMillis(), status, BaseDatum.EMPTY_METADATA_MAP);
        funcCall.getOutputCollector().add(urlDatum.toTuple());
        _numUrls += 1;
        process.increment(ImportCounters.URLS_ACCEPTED, 1);
    }
}

