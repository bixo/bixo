/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.operations;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import bixo.config.BixoPlatform;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.RedirectFetchException;
import bixo.fetcher.BaseFetcher;

@SuppressWarnings("rawtypes")
public class ResolveRedirectsTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResolveRedirectsTask.class);
    
    private String _url;
    private BaseFetcher _fetcher;
    private TupleEntryCollector _collector;
    private FlowProcess _flowProcess;

    public ResolveRedirectsTask(String url, BaseFetcher fetcher, TupleEntryCollector collector, FlowProcess flowProcess) {
        _url = url;
        _fetcher = fetcher;
        _collector = collector;
        _flowProcess = flowProcess;
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
            _collector.add(BixoPlatform.clone(new Tuple(redirectedUrl), _flowProcess));
        }
    }
}
