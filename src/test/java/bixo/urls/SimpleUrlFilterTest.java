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
package bixo.urls;

import org.junit.Assert;
import org.junit.Test;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlFilter;


public class SimpleUrlFilterTest {

    @Test
    public void testValidUrls() {
        BaseUrlFilter urlFilter = new SimpleUrlFilter();
        Assert.assertFalse(urlFilter.isRemove(new UrlDatum("http://domain.com")));
    }
    
    @Test
    public void testInvalidUrls() {
        BaseUrlFilter urlFilter = new SimpleUrlFilter();
        Assert.assertTrue("No protocol", urlFilter.isRemove(new UrlDatum("www.domain.com")));
        Assert.assertTrue("Unknown protocol", urlFilter.isRemove(new UrlDatum("mdata://www.domain.com")));
        Assert.assertTrue("Invalid port", urlFilter.isRemove(new UrlDatum("http://www.domain.com:a")));
    }
}
