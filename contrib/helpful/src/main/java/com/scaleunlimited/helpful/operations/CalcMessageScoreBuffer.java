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

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


@SuppressWarnings({"serial", "rawtypes"})
public class CalcMessageScoreBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
	private static final Logger LOGGER = LoggerFactory.getLogger(CalcMessageScoreBuffer.class);
		
	private static final Fields OUTPUT_FIELDS = new Fields(FieldNames.EMAIL_ADDRESS, FieldNames.EMAIL_NAME, FieldNames.SCORE);

	public CalcMessageScoreBuffer() {
		super(OUTPUT_FIELDS);
	}
	
    @Override
	public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
		String email = null;
		String name = null;
		double score = 0.0;
		
		Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
		while (iter.hasNext()) {
			TupleEntry entry = iter.next();
			
			String possibleEmail = entry.getString(FieldNames.EMAIL_ADDRESS);
			if (possibleEmail != null) {
				if (email != null) {
					if (!email.equals(possibleEmail)) {
						LOGGER.warn(String.format("Duplicate entry found for email addresses (%s - %s | %s)",
								entry.getString(FieldNames.MESSAGE_ID), email, possibleEmail));
					} else {
						// We occasionally get dup message ids because a msg gets archived twice. 
						// FUTURE KKr - ignore duplicate message ids. Since they typically occur in order, we
						// could do it in our parse handling code.
					}
				} else {
					email = possibleEmail;
					name = entry.getString(FieldNames.EMAIL_NAME);
				}
			}
			
			score += entry.getDouble(FieldNames.SCORE);
		}
		
		if (email != null) {
			bufferCall.getOutputCollector().add(new Tuple(email, name, score));
		}
	}

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) { }

	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) { }
}
