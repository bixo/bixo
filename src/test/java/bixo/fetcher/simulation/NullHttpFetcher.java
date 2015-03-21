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
package bixo.fetcher.simulation;

import org.apache.http.HttpStatus;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.fetcher.BaseFetcher;
import bixo.utils.ConfigUtils;

@SuppressWarnings("serial")
public class NullHttpFetcher extends BaseFetcher {

    public NullHttpFetcher() {
        super(1, new FetcherPolicy(), ConfigUtils.BIXO_TEST_AGENT);
    }
    
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException {
        throw new HttpFetchException(scoredUrl.getUrl(), "All requests throw 404 exception", HttpStatus.SC_NOT_FOUND, null);
    }

    @Override
    public void abort() {
        // Do nothing
    }

}
