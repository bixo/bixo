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
package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;

import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.IOFetchException;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.utils.ConfigUtils;

public class SimpleHttpFetcherIntegrationTest {
    
    @Test
    public final void testNoDomain() {
        BaseFetcher fetcher = new SimpleHttpFetcher(1, ConfigUtils.BIXO_IT_AGENT);
        String url = "http://www.bogusbixodomainxxxxx.com";
        
        try {
            fetcher.get(new ScoredUrlDatum(url));
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOFetchException);
        }
    }
    
}
