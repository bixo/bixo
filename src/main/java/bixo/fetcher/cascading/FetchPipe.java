package bixo.fetcher.cascading;

import bixo.IConstants;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.IScoreGenerator;
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

    public FetchPipe(Pipe urlProvider, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcherFactory factory) {
        Pipe fetch = new Pipe("fetch_pipe", urlProvider);

        fetch = new Each(fetch, new GroupFunction(IConstants.GROUPING_KEY, keyGenerator), UrlWithGroupKeyTuple.FIELDS);
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator), UrlWithScoreTuple.FIELDS);
        fetch = new GroupBy(fetch, new Fields(IConstants.GROUPING_KEY));
        fetch = new Every(fetch, new FetcherBuffer(factory), new Fields(IConstants.URL, IConstants.FETCH_STATUS, IConstants.FETCH_CONTENT));
        setTails(fetch);
    }
}
