/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.operations;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.NullContext;

import bixo.datum.UrlDatum;
import bixo.hadoop.ImportCounters;
import bixo.urls.BaseUrlFilter;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;


@SuppressWarnings("serial")
public class UrlFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
	private static final Logger LOGGER = LoggerFactory.getLogger(UrlFilter.class);
	
	private BaseUrlFilter _filter;

	private int _numFiltered;
	private int _numAccepted;
	
	public UrlFilter(BaseUrlFilter filter) {
		_filter = filter;
	}

	@SuppressWarnings("rawtypes")
    @Override
	public void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
		LOGGER.info("Starting filtering of URLs");

		_numFiltered = 0;
		_numAccepted = 0;
	}
	
	@SuppressWarnings("rawtypes")
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
	
	@SuppressWarnings("rawtypes")
    @Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> opCall) {
		LOGGER.info("Ending filtering of URLs");
		LOGGER.info(String.format("Filtered %d URLs, accepted %d URLs", _numFiltered, _numAccepted));
		
		super.cleanup(process, opCall);
	}

}
