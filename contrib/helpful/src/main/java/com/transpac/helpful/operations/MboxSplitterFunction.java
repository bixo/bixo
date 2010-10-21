package com.transpac.helpful.operations;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.ContentBytes;
import bixo.datum.FetchedDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
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
	
    public MboxSplitterFunction() {
        super(FetchedDatum.FIELDS);
    }

    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        FetchedDatum fetchedDatum = new FetchedDatum(arguments.getTuple());
        
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
	    FetchedDatum newDatum = new FetchedDatum(new TupleEntry(fetchedDatum.getTupleEntry()));
	    newDatum.setContent(new ContentBytes(safeGetAsciiBytes(email.toString())));
	    return newDatum.getTupleEntry();
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
