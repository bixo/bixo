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
package bixo.items;

import org.apache.commons.codec.binary.Base64;

import bixo.Constants;
import bixo.fetcher.FetchContent;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class FetchedContentItem {

    private Tuple _tuple;
    private static Fields FIELDS = new Fields(Constants.URL, Constants.CONTENT);

    public FetchedContentItem() {
        _tuple = new Tuple();
    }

    public FetchedContentItem(String url, FetchContent content) {
        _tuple = new Tuple();
        _tuple.add(url);
        _tuple.add(new String(Base64.encodeBase64(content.getContent())));
    }

    public Tuple toTuple() {
        return _tuple;
    }

}
