package bixo.urldb;

import bixo.Constants;
import bixo.items.UrlItem;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class LastUpdated extends BaseOperation<Tuple> implements Aggregator<Tuple> {

    public LastUpdated(Fields fields) {
        super(fields);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        Tuple tuple = aggregatorCall.getContext();
        if (tuple == null) {
            aggregatorCall.setContext(new Tuple(aggregatorCall.getArguments().getTuple()));
        } else {
            TupleEntry entry = aggregatorCall.getArguments();

            long newLast = entry.getLong(Constants.LAST_UPDATED);
            long oldLast = new TupleEntry(UrlItem.FIELDS, tuple).getLong(Constants.LAST_UPDATED);
            if (newLast > oldLast) {
                aggregatorCall.setContext(new Tuple(entry.getTuple()));
            }
        }
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        aggregatorCall.getOutputCollector().add(aggregatorCall.getContext());
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Tuple> aggregatorCall) {
        aggregatorCall.setContext(null);
    }

}
