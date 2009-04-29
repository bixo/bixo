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
package bixo.datum;

// TODO group fieldnames by classes that use them.

public interface IFieldNames {

    String URL_DB = "url_db";

    String LINE = "line";

    // url DB
//    String URL = "url";
//    String LAST_UPDATED = "lastUpdated";
//    String LAST_FETCHED = "lastFetched";

    String FETCH = "fetch";
    String SCORE = "score";
    String FETCH_STATUS = "fetchStatus";
    String CONTENT = "content";

    String GROUPING_KEY = "groupingKey";

    String FETCH_CONTENT = "fetchContent";

    String BASE_URL = "baseUrl";

    String FETECHED_URL = "fetchedUrl";

    String FETCH_TIME = "fetchTime";

    String FETCH_RATE = "fetchRate";

    String CONTENT_TYPE = "contentType";

    String TEXT = "text";

    String OUT_LINKS = "out_links";

    // urlDatum

    String SOURCE_URL = "sourceUrl";

    String SOURCE_LAST_UPDATED = "sourceLastUpdated";

    String SOURCE_LAST_FETCHED = "sourceLastFetched";

    String SOURCE_FETCH_STATUS = "sourceFetchStatus";

    // ParsedDatum
    
    String PARSED_CONTENT_URL = "parsedContentUrl";
}
