package com.transpac.helpful.operations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;


import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import com.transpac.helpful.operations.MboxSplitterFunction;

import bixo.cascading.NullContext;
import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class MboxSplitterFunctionTest {

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
		MboxSplitterFunction splitter = new MboxSplitterFunction(new Fields());
		
		FetchedDatum datum = new FetchedDatum("baseUrl", "redirectedUrl", 0, new HttpHeaders(), new BytesWritable(), "text/ascii", 0, null);
		TupleEntry value = new TupleEntry(FetchedDatum.FIELDS, datum.toTuple());
		
		when(_funcCall.getArguments()).thenReturn(value);
		splitter.operate(_process, _funcCall);
		
		verify(_collector).add(value);
	}
	
	@Test
	public void testSplitterTwoEmails() throws UnsupportedEncodingException {
		MboxSplitterFunction splitter = new MboxSplitterFunction(new Fields());

		final String mboxString = "From 1\r\rContent 1\r\rFrom 2\r\rContent 2";
		byte[] mboxContent = mboxString.getBytes("us-ascii");
		FetchedDatum datum = new FetchedDatum("baseUrl", "redirectedUrl", 0, new HttpHeaders(), new BytesWritable(mboxContent), "application/mbox", 0, null);
		TupleEntry value = new TupleEntry(FetchedDatum.FIELDS, datum.toTuple());
		
		when(_funcCall.getArguments()).thenReturn(value);
		splitter.operate(_process, _funcCall);

		verify(_collector, times(2)).add(any(TupleEntry.class));
	}
}
