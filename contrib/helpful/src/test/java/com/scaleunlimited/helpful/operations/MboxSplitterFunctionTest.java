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
package com.scaleunlimited.helpful.operations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;

import org.junit.Before;
import org.junit.Test;

import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.helpful.operations.MboxSplitterFunction;

public class MboxSplitterFunctionTest {

	@SuppressWarnings("rawtypes")
    private FlowProcess _process;
	private TupleEntryCollector _collector;
	private FunctionCall<NullContext> _funcCall;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		_process = mock(FlowProcess.class);
		_collector = mock(TupleEntryCollector.class);
		_funcCall = mock(FunctionCall.class);
		when(_funcCall.getOutputCollector()).thenReturn(_collector);
	}
	
	@Test
	public void testSplitterWithNonMbox() {
		MboxSplitterFunction splitter = new MboxSplitterFunction();
		
		FetchedDatum datum = new FetchedDatum("baseUrl", "redirectedUrl", 0, new HttpHeaders(), new ContentBytes(), "text/ascii", 0);
		TupleEntry value = new TupleEntry(datum.getTupleEntry());
		
		when(_funcCall.getArguments()).thenReturn(value);
		splitter.operate(_process, _funcCall);
		
		verify(_collector).add(value);
	}
	
	@Test
	public void testSplitterTwoEmails() throws UnsupportedEncodingException {
		MboxSplitterFunction splitter = new MboxSplitterFunction();

		final String mboxString = "From 1\r\rContent 1\r\rFrom 2\r\rContent 2";
		byte[] mboxContent = mboxString.getBytes("us-ascii");
		FetchedDatum datum = new FetchedDatum("baseUrl", "redirectedUrl", 0, new HttpHeaders(), new ContentBytes(mboxContent), "application/mbox", 0);
		TupleEntry value = new TupleEntry(datum.getTupleEntry());
		
		when(_funcCall.getArguments()).thenReturn(value);
		splitter.operate(_process, _funcCall);

		verify(_collector, times(2)).add(any(TupleEntry.class));
	}
}
