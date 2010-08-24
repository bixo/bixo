/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.fetcher.simulation;

import org.apache.http.HttpStatus;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.BaseFetchException;
import bixo.exceptions.HttpFetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.utils.ConfigUtils;

@SuppressWarnings("serial")
public class NullHttpFetcher implements IHttpFetcher {

    @Override
    public int getMaxThreads() {
        return 1;
    }

    @Override
    public FetcherPolicy getFetcherPolicy() {
        return new FetcherPolicy();
    }

    @Override
    public FetchedDatum head(ScoredUrlDatum scoredUrl) throws BaseFetchException {
        throw new HttpFetchException(scoredUrl.getUrl(), "All requests throw 404 exception", HttpStatus.SC_NOT_FOUND, null);
    }
    
    @Override
    public FetchedDatum get(ScoredUrlDatum scoredUrl) throws BaseFetchException {
        throw new HttpFetchException(scoredUrl.getUrl(), "All requests throw 404 exception", HttpStatus.SC_NOT_FOUND, null);
    }

    @Override
    public byte[] get(String url) throws BaseFetchException {
        return new byte[0];
    }

	@Override
	public UserAgent getUserAgent() {
		return ConfigUtils.BIXO_TEST_AGENT;
	}
	
    @Override
    public void abort() {
        // Do nothing
    }

}
