package bixo.operations;

import org.apache.log4j.Logger;

import bixo.datum.UrlDatum;
import bixo.hadoop.ImportCounters;
import bixo.urls.BaseUrlFilter;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class UrlFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(UrlFilter.class);
	
	private BaseUrlFilter _filter;

	private int _numFiltered;
	private int _numAccepted;
	
	public UrlFilter(BaseUrlFilter filter) {
		_filter = filter;
	}

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
		LOGGER.info("Starting filtering of URLs");

		_numFiltered = 0;
		_numAccepted = 0;
	}
	
	@Override
	public boolean isRemove(FlowProcess process, FilterCall<NullContext> filterCall) {
		UrlDatum datum = new UrlDatum(filterCall.getArguments());
		if (_filter.isRemove(datum)) {
		    process.increment(ImportCounters.URLS_FILTERED, 1);
			_numFiltered += 1;
			return true;
		} else {
            process.increment(ImportCounters.URLS_ACCEPTED, 1);
			_numAccepted += 1;
			return false;
		}
	}
	
	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> opCall) {
		LOGGER.info("Ending filtering of URLs");
		LOGGER.info(String.format("Filtered %d URLs, accepted %d URLs", _numFiltered, _numAccepted));
	}

}
