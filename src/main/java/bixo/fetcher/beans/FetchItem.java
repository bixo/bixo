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
package bixo.fetcher.beans;

import bixo.IConstants;
import bixo.tuple.UrlWithScoreTuple;
import cascading.tuple.Fields;

public class FetchItem implements Comparable<FetchItem> {
    // TODO SGr - what is this to be used for?
    private static Fields FIELDS = new Fields(IConstants.URL, IConstants.SCORE);
    
    private final UrlWithScoreTuple _urlWithScoreTuple;

    public FetchItem(UrlWithScoreTuple urlWithScoreTuple) {
        _urlWithScoreTuple = urlWithScoreTuple;
    }

    public String getUrl() {
        return _urlWithScoreTuple.getUrl();
    }

    public double getScore() {
        return _urlWithScoreTuple.getScore();
    }

    @Override
    public int compareTo(FetchItem o) {
        // Sort in reverse order, such that higher scores are first.
        if (getScore() > o.getScore()) {
            return -1;
        } else if (getScore() < o.getScore()) {
            return 1;
        } else {
            // TODO KKr - sort by URL, so that if we do a batch fetch, we're
            // fetching pages from the same area of the website.

            // TODO SG adding a simple sting comparison for now.
            return getUrl().compareTo(o.getUrl());
        }
    }

    @Override
    public String toString() {
        return getUrl() + "(" + getScore() + ")";
    }

}
