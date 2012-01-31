package bixo.operations;

import org.apache.log4j.Logger;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.RedirectFetchException;
import bixo.fetcher.BaseFetcher;

public class ResolveRedirectsTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ResolveRedirectsTask.class);
    
    private String _url;
    private BaseFetcher _fetcher;
    private TupleEntryCollector _collector;

    public ResolveRedirectsTask(String url, BaseFetcher fetcher, TupleEntryCollector collector) {
        _url = url;
        _fetcher = fetcher;
        _collector = collector;
    }

    @Override
    public void run() {
        String redirectedUrl = _url;

        try {
            FetchedDatum fd = _fetcher.get(new ScoredUrlDatum(_url));
            redirectedUrl = fd.getFetchedUrl();
            LOGGER.debug(String.format("No redirection of %s to %s", _url, redirectedUrl));
        } catch (RedirectFetchException e) {
            // We'll get this exception if the URL that's redirected by
            // a link shortening site is to a URL that gets redirected again.
            // In this case, we've captured the final URL in the exception,
            // so use that for downstream fetching.
            redirectedUrl = e.getRedirectedUrl();
            LOGGER.trace(String.format("Redirecting %s to %s", _url, redirectedUrl));
        } catch (HttpFetchException e) {
            // These are typically 404 or other such problems, so don't bother logging them.
            // We'll just silently emit the same URL for processing later.
            LOGGER.trace("Exception processing redirect for " + _url + ": " + e.getMessage(), e);
        } catch (BaseFetchException e) {
            // We might have hit a site that doesn't process HEAD requests properly,
            // so just emit the same URL for downstream fetching.
            LOGGER.debug("Exception processing redirect for " + _url + ": " + e.getMessage(), e);
        }

        synchronized (_collector) {
            // collectors aren't thread safe
            _collector.add(new Tuple(redirectedUrl));
        }
    }
}
