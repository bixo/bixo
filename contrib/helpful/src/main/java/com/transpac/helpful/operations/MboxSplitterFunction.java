package com.transpac.helpful.operations;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.FetchedDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Cascading function to split a single mbox file into N entries, one per each email. This then
 * lets us parse them as individual emails, which works better with the current Tika API, as we
 * can then use the metadata returned from the Parser.parse() call to get subject, etc.
 *
 */
@SuppressWarnings("serial")
public class MboxSplitterFunction extends BaseOperation<NullContext> implements Function<NullContext> {
	private static final Logger LOGGER = Logger.getLogger(MboxSplitterFunction.class);
	
	private static final String MBOX_MIME_TYPE = "application/mbox";
	private static final String MBOX_RECORD_DIVIDER = "From ";
	
    private Fields _metaDataFields;

    public MboxSplitterFunction() {
    	this(new Fields());
    }
    
    public MboxSplitterFunction(Fields metaDataFields) {
        super(FetchedDatum.FIELDS.append(metaDataFields));
        _metaDataFields = metaDataFields;
    }

    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple(), _metaDataFields);
        
        // Now, if the FetchedDatum mime-type is application/mbox, we want to split it into N FetchedDatum
        // tuples, one per mail message.
        if (fetchedDatum.getContentType().equals(MBOX_MIME_TYPE)) {
        	splitIntoEmails(fetchedDatum, functionCall.getOutputCollector());
        } else {
        	// Pass through as-is
        	functionCall.getOutputCollector().add(arguments);
        }
    }

	private void splitIntoEmails(FetchedDatum fetchedDatum, TupleEntryCollector outputCollector) {
		
		BufferedReader reader;
		try {
			InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes());
			reader = new BufferedReader(new InputStreamReader(is, "us-ascii"));
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("Unexpected exception while splitting mbox file", e);
			return;
		}
		
		StringBuilder email = new StringBuilder();
		for (String curLine = safeReadLine(reader); curLine != null; curLine = safeReadLine(reader)) {
			if (curLine.startsWith(MBOX_RECORD_DIVIDER)) {
				if (email.length() > 0) {
					outputCollector.add(makeNewTupleEntry(fetchedDatum, email));
				}

				email.setLength(0);
			}

			email.append(curLine);
			email.append('\r');
		}
        
        if (email.length() > 0) {
        	outputCollector.add(makeNewTupleEntry(fetchedDatum, email));
        }
	}
	
	private TupleEntry makeNewTupleEntry(FetchedDatum fetchedDatum, StringBuilder email) {
		// FUTURE KKr - add clone support to FetchedDatum, use it here.
		FetchedDatum newDatum = new FetchedDatum(fetchedDatum.getBaseUrl(), fetchedDatum.getFetchedUrl(),
				fetchedDatum.getFetchTime(), fetchedDatum.getHeaders(),
				new BytesWritable(safeGetAsciiBytes(email.toString())), MBOX_MIME_TYPE,
				fetchedDatum.getResponseRate(), fetchedDatum.getMetaDataMap());
		return new TupleEntry(FetchedDatum.FIELDS.append(fetchedDatum.getMetaDataFields()), newDatum.toTuple());
	}

	private byte[] safeGetAsciiBytes(String string) {
		try {
			return string.getBytes("us-ascii");
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("Unexpected exception while splitting mbox file", e);
			return new byte[0];
		}
	}
	
	private String safeReadLine(BufferedReader reader) {
		try {
			return reader.readLine();
		} catch (IOException e) {
			LOGGER.error("Unexpected exception while splitting mbox file", e);
			return null;
		}
	}
}
