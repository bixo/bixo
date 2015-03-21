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
