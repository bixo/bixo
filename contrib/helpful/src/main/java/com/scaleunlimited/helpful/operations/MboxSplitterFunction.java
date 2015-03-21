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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.scaleunlimited.cascading.NullContext;

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
@SuppressWarnings({"serial", "rawtypes"})
public class MboxSplitterFunction extends BaseOperation<NullContext> implements Function<NullContext> {
	private static final Logger LOGGER = LoggerFactory.getLogger(MboxSplitterFunction.class);
	
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
