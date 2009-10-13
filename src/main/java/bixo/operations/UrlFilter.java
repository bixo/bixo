package bixo.operations;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.UrlDatum;
import bixo.urldb.IUrlFilter;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class UrlFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(UrlFilter.class);
	
	private Fields _metaDataFields;
	private IUrlFilter _filter;

	private int _numFiltered;
	private int _numAccepted;
	
	public UrlFilter(IUrlFilter filter, Fields metaDataFields) {
		_metaDataFields = metaDataFields;
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
		UrlDatum datum = new UrlDatum(filterCall.getArguments().getTuple(), _metaDataFields);
		if (_filter.isRemove(datum)) {
			_numFiltered += 1;
			return true;
		} else {
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
