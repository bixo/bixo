/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.urldb;

import bixo.IConstants;
import bixo.tuple.UrlTuple;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class LastUpdated extends BaseOperation<Tuple> implements Aggregator<Tuple> {

    public LastUpdated(Fields fields) {
        super(fields);
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        aggregatorCall.setContext(null);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        Tuple tuple = aggregatorCall.getContext();
        if (tuple == null) {
            aggregatorCall.setContext(new Tuple(aggregatorCall.getArguments().getTuple()));
        } else {
            TupleEntry entry = aggregatorCall.getArguments();

            long newLast = entry.getLong(IConstants.LAST_UPDATED);
            long oldLast = new TupleEntry(UrlTuple.FIELDS, tuple).getLong(IConstants.LAST_UPDATED);
            if (newLast > oldLast) {
                aggregatorCall.setContext(new Tuple(entry.getTuple()));
            }
        }
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        aggregatorCall.getOutputCollector().add(aggregatorCall.getContext());
    }

}
