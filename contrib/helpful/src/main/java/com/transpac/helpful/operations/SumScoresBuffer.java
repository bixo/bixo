package com.transpac.helpful.operations;

import java.util.HashSet;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class SumScoresBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
	private static final Fields OUTPUT_FIELDS = new Fields(FieldNames.EMAIL_ADDRESS, FieldNames.EMAIL_NAME, FieldNames.SUMMED_SCORE);

	public SumScoresBuffer() {
		super(OUTPUT_FIELDS);
	}
	
	@Override
	public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
		double summedScore = 0.0;

		HashSet<String> names = new HashSet<String>();
				
		Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
		while (iter.hasNext()) {
			TupleEntry entry = iter.next();
			
			double score = entry.getDouble(FieldNames.SCORE);
			summedScore += score;
			
			String name = entry.getString(FieldNames.EMAIL_NAME);
			if (name != null) {
				name = name.trim();
				if (name.startsWith("\"") && name.endsWith("\"")) {
					name = name.substring(1, name.length() - 1);
				}
				
				if (!names.contains(name)) {
					names.add(name);
				}
			}
		}
		
		String emailAddress = bufferCall.getGroup().getString(FieldNames.EMAIL_ADDRESS);
		StringBuilder emailNames = new StringBuilder();
		if (names.size() > 0) {
			for (String name : names) {
				emailNames.append(name);
				emailNames.append("; ");
			}

			emailNames.setLength(emailNames.length() - 2);
		}
		
		bufferCall.getOutputCollector().add(new Tuple(emailAddress, emailNames.toString(), summedScore));
	}

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) { }

	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) { }
}
