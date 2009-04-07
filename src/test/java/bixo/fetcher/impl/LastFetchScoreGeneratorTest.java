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
package bixo.fetcher.impl;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;

import bixo.fetcher.impl.LastFetchScoreGenerator;
import bixo.items.UrlItem;

public class LastFetchScoreGeneratorTest {

    @Test
    public void testScore() throws Exception {
        long now = System.currentTimeMillis();
        LastFetchScoreGenerator generator = new LastFetchScoreGenerator(now, 10);
        UrlItem mock = Mockito.mock(UrlItem.class);
        Mockito.when(mock.getLastFetched()).thenReturn(0l);
        Assert.assertEquals(1.0, generator.generateScore(mock));
        Mockito.when(mock.getLastFetched()).thenReturn(now - 11);
        Assert.assertEquals(1.0, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now);
        Assert.assertEquals(0.0, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 5);
        Assert.assertEquals(0.5, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 1);
        Assert.assertEquals(0.1, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 9);
        Assert.assertEquals(0.9, generator.generateScore(mock));
    }
}
