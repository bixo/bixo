package bixo.exceptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.datum.HttpHeaders;
import bixo.datum.UrlStatus;

@SuppressWarnings({ "serial" })
public class HttpFetchException extends BaseFetchException implements WritableComparable<HttpFetchException> {
    private static final Logger LOGGER = Logger.getLogger(HttpFetchException.class);
    
    private int _httpStatus;
    private HttpHeaders _httpHeaders;
    
    public HttpFetchException() {
        super();
    }
    
    public HttpFetchException(String url, String msg, int httpStatus, HttpHeaders httpHeaders) {
        super(url, msg);
        _httpStatus = httpStatus;
        _httpHeaders = httpHeaders;
    }
    
    public int getHttpStatus() {
        return _httpStatus;
    }
    
    public HttpHeaders getHttpHeaders() {
        return _httpHeaders;
    }

    @Override
    public String getMessage() {
        StringBuilder result = new StringBuilder(super.getMessage());
        result.append(" (");
        result.append(_httpStatus);
        result.append(") Headers: ");
        result.append(_httpHeaders.toString());
        
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
                throw new RuntimeException("Invalid HTTP status for exception: " + _httpStatus);
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
        _httpHeaders = new HttpHeaders(input.readUTF());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        writeBaseFields(output);
        output.writeInt(_httpStatus);
        output.writeUTF(_httpHeaders.toString());
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
