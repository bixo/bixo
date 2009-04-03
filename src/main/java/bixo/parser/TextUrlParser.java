package bixo.parser;

import bixo.Constants;
import bixo.fetcher.FetchStatus;
import bixo.items.UrlItem;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class TextUrlParser extends BaseOperation<String> implements Function<String> {

    private UrlFilter[] _urlFilters = new UrlFilter[0];

    public TextUrlParser() {
        super(UrlItem.FIELDS);
    }

    public TextUrlParser(UrlFilter... urlFilters) {
        super(UrlItem.FIELDS);
        if (urlFilters != null) {
            _urlFilters = urlFilters;
        }
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<String> call) {
        TupleEntry arguments = call.getArguments();
        String url = (String) arguments.get(Constants.LINE);

        for (UrlFilter filter : _urlFilters) {
            url = filter.filter(url);
            if (url == null) {
                // ignore this url
                return;
            }
        }
        // emit link with default values
        call.getOutputCollector().add(new Tuple(url, System.currentTimeMillis(), 0, FetchStatus.NEVER_FETCHED.ordinal()));
    }
}
