package bixo.fetcher.cascading;

import bixo.Constants;
import bixo.fetcher.util.GroupingKeyGenerator;
import bixo.fetcher.util.ScoreGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {

    public FetchPipe(Pipe urlProvider, GroupingKeyGenerator keyGenerator, ScoreGenerator scoreGenerator) {
        Pipe fetch = new Pipe("fetch pipe", urlProvider);
        fetch = new Each(fetch, new GroupFunction(Constants.GROUPING_KEY, keyGenerator));
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator));
        fetch = new GroupBy(fetch, new Fields(Constants.GROUPING_KEY));
        fetch = new Every(fetch, new FetcherBuffer());
        setTails(fetch);
    }
}
