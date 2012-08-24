package bixo.exceptions;

import static org.junit.Assert.*;

import org.junit.Test;

import bixo.datum.HttpHeaders;

public class HttpFetchExceptionTest {

    @Test
    public void testToString() {
        HttpFetchException e = new HttpFetchException("url", "msg", 300, new HttpHeaders());
        assertEquals("HttpFetchException: msg (300) [url]", e.toString());
    }

    @Test
    public void testHeadersInMsg() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("key", "value");
        HttpFetchException e = new HttpFetchException("url", "msg", 300, httpHeaders);
        assertEquals("HttpFetchException: msg (300) Headers: key=value [url]", e.toString());
    }

}
