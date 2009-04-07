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
package bixo.fetcher;

public class FetchContent {
    private String _baseUrl;
    private String _fetchedUrl;
    private long _fetchTime;
    
    private byte[] _content;
    private String _contentType;
    
    public FetchContent(String baseUrl, String fetchedUrl, long fetchTime, byte[] content, String contentType) {
        _baseUrl = baseUrl;
        _fetchedUrl = fetchedUrl;
        _fetchTime = fetchTime;
        _content = content;
        _contentType = contentType;
    }

    public String getBaseUrl() {
        return _baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        _baseUrl = baseUrl;
    }

    public String getFetchedUrl() {
        return _fetchedUrl;
    }

    public void setFetchedUrl(String fetchedUrl) {
        _fetchedUrl = fetchedUrl;
    }

    public long getFetchTime() {
        return _fetchTime;
    }

    public void setFetchTime(long fetchTime) {
        _fetchTime = fetchTime;
    }

    public byte[] getContent() {
        return _content;
    }

    public void setContent(byte[] content) {
        _content = content;
    }

    public String getContentType() {
        return _contentType;
    }

    public void setContentType(String contentType) {
        _contentType = contentType;
    }
    
    
}
