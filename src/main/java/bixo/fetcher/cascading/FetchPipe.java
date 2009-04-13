package bixo.fetcher.cascading;

import bixo.Constants;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.util.GroupingKeyGenerator;
import bixo.fetcher.util.ScoreGenerator;
import bixo.tuple.FetchResultTuple;
import bixo.tuple.UrlWithGroupKeyTuple;
import bixo.tuple.UrlWithScoreTuple;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {

    public FetchPipe(Pipe urlProvider, GroupingKeyGenerator keyGenerator, ScoreGenerator scoreGenerator, IHttpFetcherFactory factory) {
        Pipe fetch = new Pipe("fetch_pipe", urlProvider);

        fetch = new Each(fetch, new GroupFunction(Constants.GROUPING_KEY, keyGenerator), UrlWithGroupKeyTuple.FIELDS);
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator), UrlWithScoreTuple.FIELDS);
        fetch = new GroupBy(fetch, new Fields(Constants.GROUPING_KEY));
        fetch = new Every(fetch, new FetcherBuffer(factory), FetchResultTuple.FIELDS);
        setTails(fetch);
    }
}
