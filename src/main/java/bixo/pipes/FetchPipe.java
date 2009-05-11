package bixo.pipes;

import bixo.datum.FetchedDatum;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.NormalizedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.IGroupingKeyGenerator;
import bixo.fetcher.util.IScoreGenerator;
import bixo.operations.FetcherBuffer;
import bixo.operations.GroupFunction;
import bixo.operations.NormalizeFunction;
import bixo.operations.ScoreFunction;
import bixo.urldb.IUrlNormalizer;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class FetchPipe extends SubAssembly {

    public FetchPipe(Pipe urlProvider, IUrlNormalizer urlNormalizer, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcher fetcher) {
        this(urlProvider, urlNormalizer, keyGenerator, scoreGenerator, fetcher, new Fields());
    }

    public FetchPipe(Pipe urlProvider, IUrlNormalizer urlNormalizer, IGroupingKeyGenerator keyGenerator, IScoreGenerator scoreGenerator, IHttpFetcher fetcher, Fields metaDataFields) {

        Pipe fetch = new Pipe("fetch_pipe", urlProvider);

        Fields normalizedFields = NormalizedUrlDatum.FIELDS.append(metaDataFields);
        fetch = new Each(fetch, new NormalizeFunction(UrlDatum.URL_FIELD, new Fields(NormalizedUrlDatum.NORMALIZED_URL_FIELD), urlNormalizer), normalizedFields);
        
        Fields groupedFields = GroupedUrlDatum.FIELDS.append(metaDataFields);
        fetch = new Each(fetch, new GroupFunction(metaDataFields, keyGenerator), groupedFields);

        Fields scoreFields = ScoredUrlDatum.FIELDS.append(metaDataFields);
        fetch = new Each(fetch, new ScoreFunction(scoreGenerator, metaDataFields), scoreFields);

        fetch = new GroupBy(fetch, new Fields(GroupedUrlDatum.GROUP_KEY_FIELD));
        fetch = new Every(fetch, new FetcherBuffer(FetchedDatum.FIELDS, metaDataFields, fetcher), Fields.RESULTS);

        setTails(fetch);
    }
}
