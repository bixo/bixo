package bixo.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class HtmlUtilsTest {
    
    @Test
    public void testNoFollowMetaTags() {
        final String htmlText1 = "<html><head><meta name=\"ROBOTS\" content=\"NONE\" /></head><body></body></html>";
        final String htmlText2 = "<html><head><meta name=\"ROBOTS\" content=\"nofollow\" /></head><body></body></html>";
        
        assertTrue(HtmlUtils.hasNoFollowMetaTags(htmlText1));
        assertTrue(HtmlUtils.hasNoFollowMetaTags(htmlText2));
    }
    
    
    @Test
    public void testNoArchiveMetaTags() {
        final String htmlText1 = "<html><head><meta name=\"ROBOTS\" content=\"NONE\" /></head><body></body></html>";
        
        final String htmlText2 = "<html><head><meta name=\"ROBOTS\" content=\"NoArchive\" /></head><body></body></html>";
        
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
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText3));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText4));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText5));
        assertTrue(HtmlUtils.hasNoArchiveMetaTags(htmlText6));
    }
    
    
}
