package bixo.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class URLNormalizerTest {
    private URLNormalizer _normalizer;
    
    private void normalizeTest(String weird, String normal, String testName) {
        Assert.assertEquals(testName, normal, _normalizer.normalize(weird));
    }

    @Before
    public void setupNormalizer() {
        _normalizer = new URLNormalizer();
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

        normalizeTest("www.foo.com", "http://www.foo.com", "Add 'http://' protocol to raw URL");
        normalizeTest("mailto://ken@foo.com", "mailto://ken@foo.com", "Don't add 'http://' protocol to URLs with other protocols");

        normalizeTest("http://www.foo.com/%66oo.html", "http://www.foo.com/foo.html", "Convert safe encoded characters to actual characters");
        normalizeTest("http://www.foo.com/foo?q=%66oo", "http://www.foo.com/foo?q=foo", "Convert safe encoded characters to actual characters (in query)");
        normalizeTest("http://www.foo.com/foo?q=", "http://www.foo.com/foo?q=", "Handle empty value in query parameter");
        normalizeTest("http://www.foo.com/foo?q", "http://www.foo.com/foo?q", "Handle no value in query parameter");
        normalizeTest("http://www.foo.com/foo?q&p&r=&&s=t", "http://www.foo.com/foo?q&p&r=&&s=t", "Handle funky values in query parameter");
        normalizeTest("http://www.foo.com/foo%20me.html", "http://www.foo.com/foo+me.html", "Convert encoded spaces to '+' format");
        normalizeTest("http://www.foo.com/foo%3Fme.html", "http://www.foo.com/foo%3Fme.html", "Don't convert special chars from hex encoding");
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
}
