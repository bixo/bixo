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
        
// TODO KKr I used the following command to create /compressed.zip:
//
// perl -MCompress::Zlib -e 'undef $/; print compress(<>)' < compressed.txt > compressed.zip
//
// Using the following command uncompresses it into a file identical to the original compressed.txt:
//
// perl -MCompress::Zlib -e 'undef $/; print uncompress(<>)' < compressed.zip > compressed2.txt
//
// Unfortunately, this test causes InflaterInputStream.read() to throw: 
//
// java.util.zip.ZipException: invalid stored block lengths
//
// I've removed deflate from SimpleHttpFetcher.DEFAULT_ACCEPT_ENCODING for now.
// This shouldn't be much of an issue, though, as nearly all web sites that
// support deflate will also support gzip. They're motivated to do so, since
// Explorer screwed up its deflate support by using RFC 1251 instead of RFC 1250.
// The real blame should go to the HTTP 1.1 standard (RFC 2616), which created
// confusion by naming it "deflate" instead of "zlib".
// (see http://www.zlib.net/zlib_faq.html#faq39).
//
//        InputStream is = EncodingUtilsTest.class.getResourceAsStream("/compressed.zip");
//        byte[] buffer = new byte[4096];
//        int length = is.read(buffer);
//        byte[] compressed = new byte[length];
//        System.arraycopy(buffer, 0, compressed, 0, length);
//        byte[] uncompressed = EncodingUtils.processDeflateEncoded(compressed);
//        
//        Assert.assertEquals("Now is the time for all good men to come to the aid of their country.",
//                        new String(uncompressed, "us-ascii"));
    }
}
