package bixo.utils;

import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;


public class EncodingUtilsTest {

    @Test
    public void testGzip() throws Exception {
        InputStream is = EncodingUtilsTest.class.getResourceAsStream("/compressed.gz");
        byte[] buffer = new byte[4096];
        int length = is.read(buffer);
        byte[] compressed = new byte[length];
        System.arraycopy(buffer, 0, compressed, 0, length);
        byte[] uncompressed = EncodingUtils.processGzipEncoded(compressed);
        
        Assert.assertEquals("Now is the time for all good men to come to the aid of their country.",
                        new String(uncompressed, "us-ascii"));
    }
    
    @Test
    public void testDeflate() throws Exception {
        
    }
}
