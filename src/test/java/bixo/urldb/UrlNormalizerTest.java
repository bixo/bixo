package bixo.urldb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import bixo.urldb.UrlNormalizer;


public class UrlNormalizerTest {
    private UrlNormalizer _normalizer;
    
    private void normalizeTest(String weird, String normal, String testName) {
        Assert.assertEquals(testName, normal, _normalizer.normalize(weird));
    }

    @Before
    public void setupNormalizer() {
        _normalizer = new UrlNormalizer();
    }
    
    @Test
    public void testNormalizer() {
        normalizeTest(" http://www.foo.com/ ", "http://www.foo.com/", "remove leading/trailing spaces");
        normalizeTest("HTTP://www.foo.com/", "http://www.foo.com/", "lower-case protocol");
        normalizeTest("http://WWW.Foo.Com/index.html", "http://www.foo.com/index.html", "lower-case host");

        normalizeTest("http://www.foo.com:80/index.html", "http://www.foo.com/index.html", "remove default port");
        normalizeTest("https://www.foo.com:443/index.html", "https://www.foo.com/index.html", "remove default port");
        normalizeTest("http://www.foo.com:81/", "http://www.foo.com:81/", "don't remove custom port");

        normalizeTest("http://www.foo.com", "http://www.foo.com/", "Add final '/' to hostname if no path");
        normalizeTest("http://www.foo.com/foo?mode=html", "http://www.foo.com/foo?mode=html", "leave query");

        normalizeTest("http://www.foo.com/foo.html#ref", "http://www.foo.com/foo.html", "remove reference");
        normalizeTest("http://www.foo.com/foo?q=query#ref", "http://www.foo.com/foo?q=query", "remove reference from query");

        normalizeTest("http://foo.com/", "http://www.foo.com/", "add 'www.' to hostname with only paid-level domain");
        normalizeTest("http://foo.co.jp/", "http://www.foo.co.jp/", "add 'www.' to hostname with only paid-level domain");
        normalizeTest("https://foo.com/", "https://www.foo.com/", "add 'www.' to hostname with only paid-level domain");
        normalizeTest("http://aws.foo.com/", "http://aws.foo.com/", "don't add 'www.' to hostnames with sub-domains before PLD domain");
        normalizeTest("ftp://foo.com", "ftp://foo.com", "Don't add 'www.' to non-http protocol domains");

        normalizeTest("www.foo.com/", "http://www.foo.com/", "Add 'http://' protocol to raw URL");
        normalizeTest("mailto://ken@foo.com", "mailto://ken@foo.com", "Don't add 'http://' protocol to URLs with other protocols");

        normalizeTest("http://www.foo.com/%66oo.html", "http://www.foo.com/foo.html", "Convert safe encoded characters to actual characters");
        normalizeTest("http://www.foo.com/foo?q=%66oo", "http://www.foo.com/foo?q=foo", "Convert safe encoded characters to actual characters (in query)");
        normalizeTest("http://www.foo.com/foo?q=", "http://www.foo.com/foo?q=", "Handle empty value in query parameter");
        normalizeTest("http://www.foo.com/foo?q", "http://www.foo.com/foo?q", "Handle no value in query parameter");
        normalizeTest("http://www.foo.com/foo?q&p&r=&&s=t", "http://www.foo.com/foo?q&p&r=&&s=t", "Handle funky values in query parameter");
        normalizeTest("http://www.foo.com/foo%20me.html", "http://www.foo.com/foo+me.html", "Convert encoded spaces to '+' format");
        normalizeTest("http://www.foo.com/foo%3Fme.html", "http://www.foo.com/foo%3fme.html", "Don't convert special chars from hex encoding");
        normalizeTest("http://www.foo.com/foo/bar.html", "http://www.foo.com/foo/bar.html", "Don't convert path separators");
        
        /* FUTURE uncomment these when BIXO-56 is done
         
        // check that unnecessary "../" are removed
        normalizeTest("http://www.foo.com/aa/../", 
                      "http://www.foo.com/", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/bb/../",
                      "http://www.foo.com/aa/", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/..",
                      "http://www.foo.com/aa/..", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/bb/cc/../../foo.html",
                      "http://www.foo.com/aa/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/bb/../cc/dd/../ee/foo.html",
                      "http://www.foo.com/aa/cc/ee/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/../../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/../aa/../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/../../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/../bb/../foo.html/../../",
                      "http://www.foo.com/", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/../aa/foo.html",
                      "http://www.foo.com/aa/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/../aa/../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/a..a/foo.html",
                      "http://www.foo.com/a..a/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/a..a/../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/foo.foo/../foo.html",
                      "http://www.foo.com/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com//aa/bb/foo.html",
                      "http://www.foo.com/aa/bb/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa//bb/foo.html",
                      "http://www.foo.com/aa/bb/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com/aa/bb//foo.html",
                      "http://www.foo.com/aa/bb/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com//aa//bb//foo.html",
                      "http://www.foo.com/aa/bb/foo.html", "Remove unnecessary '../' sequences");
        normalizeTest("http://www.foo.com////aa////bb////foo.html",
                      "http://www.foo.com/aa/bb/foo.html", "Remove unnecessary '../' sequences");
        */
    }
    
    @Test
    public void testStumbleUponURLs() {
        normalizeTest("http://www.stumbleupon.com/toolbar/#url=http%3A//links.flashdance.cx/misc-pix/fjortisfangelse.jpg",
                        "http://www.stumbleupon.com/toolbar/#url=http%3a//links.flashdance.cx/misc-pix/fjortisfangelse.jpg",
        "Preserve pseudo-anchor used as qeury");

        normalizeTest("http://www.stumbleupon.com/toolbar/#topic=Poetry&url=http%3A//www.notellmotel.org/poem_single.php%3Fid%3D78_0_1_0",
                        "http://www.stumbleupon.com/toolbar/#topic=Poetry&url=http%3a//www.notellmotel.org/poem_single.php%3fid%3d78_0_1_0",
        "Preserve pseudo-anchor and required encoded chars");
    }
    
    @Test
    public void testSpaceEncoding() {
        normalizeTest("http://www.domain.com/some text", "http://www.domain.com/some+text", "Encode spaces as '+' chars");
        normalizeTest("http://www.domain.com/some+text", "http://www.domain.com/some+text", "Preserve '+' chars");
        normalizeTest("http://www.domain.com/some%20text", "http://www.domain.com/some+text", "Normalize %20 as '+'");
    }
    
    @Test
    public void testForwardSlashes() {
        normalizeTest("http://www.domain.com/some%2ftext", "http://www.domain.com/some%2ftext", "Preserve encoded slashes");
        normalizeTest("http://www.domain.com/?q=some%2ftext", "http://www.domain.com/?q=some/text", "Don't encode slashes in query text");
    }
    
    @Test
    public void testIPAddress() {
        normalizeTest("http://209.85.173.132/search?q=cache", "http://209.85.173.132/search?q=cache", "Don't add www to IP address");
    }
    
    @Test
    public void testMultipleValuesInQuery() {
        normalizeTest("http://delivery.vipeers.com/file_sharing?m=7Q==&locale=en", "http://delivery.vipeers.com/file_sharing?m=7Q==&locale=en", "Preserve empty values");
        normalizeTest("http://delivery.vipeers.com/file_sharing?m=7Q=full=&locale=en", "http://delivery.vipeers.com/file_sharing?m=7Q=full=&locale=en", "Preserve empty values");
        normalizeTest("http://delivery.vipeers.com/file_sharing?m=7Q=full=fast&locale=en", "http://delivery.vipeers.com/file_sharing?m=7Q=full=fast&locale=en", "Preserve multi values");
        normalizeTest("http://delivery.vipeers.com/file_sharing?m=7Q==fast&locale=en", "http://delivery.vipeers.com/file_sharing?m=7Q==fast&locale=en", "Preserve empty values");
    }
    
    @Test
    public void testAddingTrailingSlash() {
        normalizeTest("http://www.domain.com", "http://www.domain.com/", "Add trailing slash");
        normalizeTest("www.pondliner.com", "http://www.pondliner.com/", "Add trailing slash even if no protocol");
    }
}
