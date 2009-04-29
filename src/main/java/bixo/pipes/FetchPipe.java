package bixo.pipes;

import bixo.IConstants;
import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.IScoreGenerator;
import bixo.operations.FetcherBuffer;
import bixo.operations.GroupFunction;
import bixo.operations.ScoreFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {

    public FetchPipe(Pipe urlProvider, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcherFactory factory) {
        this(urlProvider, keyGenerator, scoreGenerator, factory, new Fields());
    }

    public FetchPipe(Pipe urlProvider, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcherFactory factory, Fields metaDataFields) {

        Pipe fetch = new Pipe("fetch_pipe", urlProvider);
        String groupingKey = IConstants.GROUPING_KEY;

        Fields groupedFields = GroupedUrlDatum.getFields().append(metaDataFields);
        fetch = new Each(fetch, new GroupFunction(groupingKey, metaDataFields, keyGenerator), groupedFields);

        Fields scoreFields = ScoredUrlDatum.getFields().append(metaDataFields);
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator, metaDataFields), scoreFields);

        fetch = new GroupBy(fetch, new Fields(groupingKey));

        Fields fetchFields = FetchedDatum.getFields().append(metaDataFields);
        fetch = new Every(fetch, new FetcherBuffer(FetchedDatum.getFields(), metaDataFields, factory), fetchFields);

        setTails(fetch);
    }
}
