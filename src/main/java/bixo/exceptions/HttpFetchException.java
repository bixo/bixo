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
package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.datum.HttpHeaders;
import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class HttpFetchException extends BaseFetchException implements WritableComparable<HttpFetchException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpFetchException.class);
    
    private int _httpStatus;
    private HttpHeaders _httpHeaders;
    
    public HttpFetchException() {
        super();
    }
    
    public HttpFetchException(String url, String msg, int httpStatus, HttpHeaders httpHeaders) {
        super(url, buildMessage(msg, httpStatus, httpHeaders));
        _httpStatus = httpStatus;
        _httpHeaders = httpHeaders;
    }
    
    public int getHttpStatus() {
        return _httpStatus;
    }
    
    public HttpHeaders getHttpHeaders() {
        return _httpHeaders;
    }

    private static String buildMessage(String msg, int httpStatus, HttpHeaders httpHeaders) {
        StringBuilder result = new StringBuilder(msg);
        result.append(" (");
        result.append(httpStatus);
        result.append(")");
        
        String headers = httpHeaders.toString();
        if (headers.length() > 0) {
            result.append(" Headers: ");
            result.append(headers);
        }
        
        return result.toString();
    }
    
    @Override
    public UrlStatus mapToUrlStatus() {
        switch (_httpStatus) {
        case HttpStatus.SC_FORBIDDEN:
            return UrlStatus.HTTP_FORBIDDEN;

        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED:
            return UrlStatus.HTTP_UNAUTHORIZED;
            
        case HttpStatus.SC_NOT_FOUND:
            return UrlStatus.HTTP_NOT_FOUND;

        case HttpStatus.SC_GONE:
            return UrlStatus.HTTP_GONE;
            
        case HttpStatus.SC_TEMPORARY_REDIRECT:
        case HttpStatus.SC_MOVED_TEMPORARILY:
        case HttpStatus.SC_SEE_OTHER:
            return UrlStatus.HTTP_TOO_MANY_REDIRECTS;
            
        case HttpStatus.SC_MOVED_PERMANENTLY:
            return UrlStatus.HTTP_MOVED_PERMANENTLY;
            
        default:
            if (_httpStatus < 300) {
                LOGGER.warn("Invalid HTTP status for exception: " + _httpStatus);
                return UrlStatus.HTTP_SERVER_ERROR;
            } else if (_httpStatus < 400) {
                return UrlStatus.HTTP_REDIRECTION_ERROR;
            } else if (_httpStatus < 500) {
                return UrlStatus.HTTP_CLIENT_ERROR;
            } else if (_httpStatus < 600) {
                return UrlStatus.HTTP_SERVER_ERROR;
            } else {
                LOGGER.warn("Unknown HTTP status for exception: " + _httpStatus);
                return UrlStatus.HTTP_SERVER_ERROR;
            }
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        readBaseFields(input);
        
        _httpStatus = input.readInt();
        _httpHeaders = new HttpHeaders();
        _httpHeaders.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        writeBaseFields(output);
        output.writeInt(_httpStatus);
        _httpHeaders.write(output);
    }

    @Override
    public int compareTo(HttpFetchException e) {
        int result = compareToBase(e);
        if (result == 0) {
            if (_httpStatus < e._httpStatus) {
                result = -1;
            } else if (_httpStatus > e._httpStatus) {
                result = 1;
            } else {
                result = _httpHeaders.toString().compareTo(e._httpHeaders.toString());
            }
        }
        
        return result;
    }
}
