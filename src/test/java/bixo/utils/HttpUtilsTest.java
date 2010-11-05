package bixo.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class HttpUtilsTest {

    @Test
    public void extractMimeTypeTest() {
        assertEquals("text/plain", HttpUtils.getMimeTypeFromContentType("text/plain"));
        assertEquals("text/xml", HttpUtils.getMimeTypeFromContentType("text/xml; charset=UTF-8"));
        assertEquals("text/plain", HttpUtils.getMimeTypeFromContentType(" text/plain ; charset=UTF-8"));
        assertEquals("", HttpUtils.getMimeTypeFromContentType(""));
    }

    @Test
    public void extractCharsetTest() {
        assertEquals("", HttpUtils.getCharsetFromContentType("text/plain"));
        assertEquals("UTF-8", HttpUtils.getCharsetFromContentType("text/xml; charset=UTF-8"));
        // TODO KKr - reenable this test when Tika is fixed up
         assertEquals("us-ascii", HttpUtils.getCharsetFromContentType("text/xml;CHARSET = us-ascii "));
    }
}
