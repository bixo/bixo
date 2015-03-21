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

public class HtmlUtilsTest {
    
    @Test
    public void testNoFollowMetaTags() {
        final String htmlText1 = "<html><head><meta name=\"ROBOTS\" content=\"NONE\" /></head><body></body></html>";
        final String htmlText2 = "<html><head><meta name=\"ROBOTS\" content=\"nofollow\" /></head><body></body></html>";
        final String htmlText3 = "<html><head><meta name=\"ROBOTS\" content=\"NOINDEX,NOFOLLOW\" /></head><body></body></html>";
        
        assertTrue(HtmlUtils.hasNoFollowMetaTags(htmlText1));
        assertTrue(HtmlUtils.hasNoFollowMetaTags(htmlText2));
        assertTrue(HtmlUtils.hasNoFollowMetaTags(htmlText3));
    }
    
    
    @Test
    public void testNoArchiveMetaTags() {
        final String htmlText1 = "<html><head><meta name=\"ROBOTS\" content=\"NONE\" /></head><body></body></html>";
        final String htmlText2 = "<html><head><meta name=\"ROBOTS\" content=\"NoArchive\" /></head><body></body></html>";
        final String htmlText21 = "<html><head><meta name=\"ROBOTS\" content=\"NOINDEX,NOARCHIVE\" /></head><body></body></html>";
        
        // <META HTTP-EQUIV="PRAGMA" CONTENT="NO-CACHE">
        final String htmlText3 = "<html><head><meta HTTP-EQUIV=\"PRAGMA\" content=\"no-cache\" /></head><body></body></html>";
        
        // <META HTTP-EQUIV="CACHE-CONTROL" CONTENT="NO-CACHE">
        final String htmlText4 = "<html><head><meta HTTP-EQUIV=\"CACHE-CONTROL\" content=\"no-cache\" /></head><body></body></html>";

        // <META HTTP-EQUIV="CACHE-CONTROL" CONTENT="NO-STORE">
        final String htmlText5 = "<html><head><meta HTTP-EQUIV=\"CACHE-CONTROL\" content=\"NO-STORE\" /></head><body></body></html>";

        // <META HTTP-EQUIV="CACHE-CONTROL" CONTENT="PRIVATE">
        final String htmlText6 = "<html><head><meta HTTP-EQUIV=\"CACHE-CONTROL\" content=\"Private\" /></head><body></body></html>";

        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText1));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText2));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText21));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText3));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText4));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText5));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText6));
    }
    
    @Test
    public void testOnlyNonEnglishMetaTags() {

        // <META HTTP-EQUIV="PRAGMA" CONTENT="NO-CACHE">
        final String htmlText1 = "<html><head><meta HTTP-EQUIV=\"PRAGMA\" content=\"no-cache\" /></head><body></body></html>";
        
        // <META HTTP-EQUIV="CONTENT-LANGUAGE" CONTENT="XXX">
        final String htmlText11 = "<html><head><meta http-equiv=\"CONTENT-LANGUAGE\" content=\"en-US\" /></head><body></body></html>";
        final String htmlText12 = "<html><head><meta http-equiv=\"CONTENT-LANGUAGE\" content=\"en\" /></head><body></body></html>";
        final String htmlText13 = "<html><head><meta http-equiv=\"CONTENT-LANGUAGE\" content=\"en-BR,fr\" /></head><body></body></html>";
        final String htmlText14 = "<html><head><meta http-equiv=\"CONTENT-LANGUAGE\" content=\"fr,ru\" /></head><body></body></html>";
        final String htmlText15 = "<html><head><meta http-equiv=\"CONTENT-LANGUAGE\" content=\"RU,en,fr\" /></head><body></body></html>";

        // <META NAME="DC.LANGUAGE" CONTENT="XXX">
        final String htmlText21 = "<html><head><meta name=\"DC.LANGUAGE\" content=\"en-US\" /></head><body></body></html>";
        final String htmlText22 = "<html><head><meta name=\"DC.LANGUAGE\" content=\"en\" /></head><body></body></html>";
        final String htmlText23 = "<html><head><meta name=\"DC.LANGUAGE\" content=\"en-BR;fr\" /></head><body></body></html>";
        final String htmlText24 = "<html><head><meta name=\"DC.LANGUAGE\" content=\"fr;ru\" /></head><body></body></html>";
        final String htmlText25 = "<html><head><meta name=\"DC.LANGUAGE\" content=\"RU;en;fr\" /></head><body></body></html>";
        
        // <META HTTP-EQUIV="CONTENT-TYPE" CONTENT="XXX">
        final String htmlText31 = "<html><head><meta http-equiv=\"CONTENT-TYPE\" content=\"text/html; charset=windows-1251\" /></head><body></body></html>";
        final String htmlText32 = "<html><head><meta http-equiv=\"CONTENT-TYPE\" content=\"text/html; charset=iso-8859-1\" /></head><body></body></html>";
        
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText1));
        
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText11));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText12));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText13));
        assertTrue(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText14));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText15));
        
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText21));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText22));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText23));
        assertTrue(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText24));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText25));
        
        assertTrue(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText31));
        assertFalse(HtmlUtils.hasOnlyNonEnglishMetaTags(htmlText32));
    }
}
