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
package bixo.urls;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import bixo.urls.BaseUrlNormalizer;
import bixo.urls.SimpleUrlNormalizer;


public class SimpleUrlNormalizerTest {
    private SimpleUrlNormalizer _normalizer;
    
    private void normalizeTest(String weird, String normal, String testName) {
    	normalizeTest(_normalizer, weird, normal, testName);
    }

    private void normalizeTest(BaseUrlNormalizer normalizer, String weird, String normal, String testName) {
        Assert.assertEquals(testName + ": " + weird, normal, normalizer.normalize(weird));
    }

    @Before
    public void setupNormalizer() {
        _normalizer = new SimpleUrlNormalizer();
    }
    
    @Test
    public void testNormalizer() {
        normalizeTest(" http://www.foo.com/ ", "http://www.foo.com/", "remove leading/trailing spaces");
        normalizeTest("HTTP://www.foo.com/", "http://www.foo.com/", "lower-case protocol");

        // Ports
        normalizeTest("http://www.foo.com:80/page.html", "http://www.foo.com/page.html", "remove default port");
        normalizeTest("https://www.foo.com:443/page.html", "https://www.foo.com/page.html", "remove default port");
        normalizeTest("http://www.foo.com:81/", "http://www.foo.com:81/", "don't remove custom port");

        // Slashes
        normalizeTest("http://www.foo.com", "http://www.foo.com/", "Add final '/' to hostname if no path");
        normalizeTest("http://www.foo.com?", "http://www.foo.com/", "Add final '/' to hostname if no path (even if trailing query)");
        normalizeTest("http://www.foo.com", "http://www.foo.com/", "Add final '/' to hostname if no path");
        normalizeTest("http://www.foo.com//bar", "http://www.foo.com/bar", "Remove doubled slashes");
        normalizeTest("http://www.foo.com//", "http://www.foo.com/", "Remove doubled slashes");

        // References
        normalizeTest("http://www.foo.com/foo.html#ref", "http://www.foo.com/foo.html", "remove reference");
        normalizeTest("http://www.foo.com/#ref", "http://www.foo.com/", "remove reference (on path)");
        normalizeTest("http://www.foo.com/foo?q=query#ref", "http://www.foo.com/foo?q=query", "remove reference (from query)");

        // Domain name
        normalizeTest("http://WWW.Foo.Com/page.html", "http://www.foo.com/page.html", "lower-case domain");
        normalizeTest("http://www.foo.com./page.html", "http://www.foo.com/page.html", "remove trailing '.' from domain");
        // normalizeTest("http://foo.com/", "http://www.foo.com/", "add 'www.' to hostname with only paid-level domain");
//        normalizeTest("http://foo.co.jp/", "http://www.foo.co.jp/", "add 'www.' to hostname with only paid-level domain");
//        normalizeTest("https://foo.com/", "https://www.foo.com/", "add 'www.' to hostname with only paid-level domain");
//        normalizeTest("http://aws.foo.com/", "http://aws.foo.com/", "don't add 'www.' to hostnames with sub-domains before PLD domain");
//        normalizeTest("ftp://foo.com", "ftp://foo.com", "Don't add 'www.' to non-http protocol domains");

        // Protocol
        normalizeTest("www.foo.com/", "http://www.foo.com/", "Add 'http://' protocol to raw URL");
        normalizeTest("mailto://ken@foo.com", "mailto://ken@foo.com", "Don't add 'http://' protocol to URLs with other protocols");

        // Encoded characters
        normalizeTest("http://www.foo.com/%66oo.html", "http://www.foo.com/foo.html", "Convert safe encoded characters to actual characters");
        normalizeTest("http://www.foo.com/foo?q=%66oo", "http://www.foo.com/foo?q=foo", "Convert safe encoded characters to actual characters (in query)");
        
        // Query string
        normalizeTest("http://www.foo.com/foo?mode=html", "http://www.foo.com/foo?mode=html", "leave query");
        normalizeTest("http://www.foo.com/bar?", "http://www.foo.com/bar", "Remove empty trailing query");
        normalizeTest("http://www.foo.com/foo?q=", "http://www.foo.com/foo?q=", "Handle empty value in query parameter");
        normalizeTest("http://www.foo.com/foo?q", "http://www.foo.com/foo?q", "Handle no value in query parameter");
        normalizeTest("http://www.foo.com/foo?q&p&r=&&s=t", "http://www.foo.com/foo?q&p&r=&s=t", "Handle funky values in query parameter");
        normalizeTest("http://www.foo.com/foo%20me.html", "http://www.foo.com/foo+me.html", "Convert encoded spaces to '+' format");
        normalizeTest("http://www.foo.com/foo%3Fme.html", "http://www.foo.com/foo%3fme.html", "Don't convert special chars from hex encoding");
        normalizeTest("http://www.foo.com/foo/bar.html", "http://www.foo.com/foo/bar.html", "Don't convert path separators");
    }
    
    @Test
    public void testRelativePathNormalization() {

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
    }
    
    @Test
    public void testSessionIDRemoval() {
        // Test simple removal of session id, keeping parameters before and after
        normalizeTest(  "http://www.foo.com/foo.php?PHPSESSID=cdc993a493e899bed04f4d0c8a462a03",
                        "http://www.foo.com/foo.php",
                        "Remove session id");
        normalizeTest(  "http://www.foo.com/foo.php?f=2&PHPSESSID=cdc993a493e899bed04f4d0c8a462a03",
                        "http://www.foo.com/foo.php?f=2",
                        "Remove session id following valid query parameter");
        normalizeTest(  "http://www.foo.com/foo.php?f=2&PHPSESSID=cdc993a493e899bed04f4d0c8a462a03&q=3",
                        "http://www.foo.com/foo.php?f=2&q=3",
                        "Remove session id in middle of valid query parameters");
        normalizeTest(  "http://www.foo.com/foo.php?PHPSESSID=cdc993a493e899bed04f4d0c8a462a03&f=2",
                        "http://www.foo.com/foo.php?f=2",
                        "Remove session id before valid query parameter.");

        // test removal of different session ids including removal of ; in jsessionid
        normalizeTest(  "http://www.foo.com/foo.php?Bv_SessionID=fassassddsajkl",
                        "http://www.foo.com/foo.php",
                        "Remove Bv_ session id");
        normalizeTest(  "http://www.foo.com/foo.php?Bv_SessionID=fassassddsajkl&x=y",
                        "http://www.foo.com/foo.php?x=y",
                        "Remove Bv_ session id before valid query parameter");
        normalizeTest(  "http://www.foo.com/foo.html;jsessionid=1E6FEC0D14D044541DD84D2D013D29ED",
                        "http://www.foo.com/foo.html",
                        "Remove jsession id");
        normalizeTest(  "http://www.foo.com/foo.html?param=1&another=2;jsessionid=1E6FEC0D14D044541DD84D2D013D29ED",
                        "http://www.foo.com/foo.html?param=1&another=2",
                        "Remove jsession id with leading ';' after valid query parameters");
        normalizeTest(  "http://www.foo.com/foo.html;jsessionid=1E6FEC0D14D044541DD84D2D013D29ED?param=1&another=2",
                        "http://www.foo.com/foo.html?param=1&another=2",
                        "Remove jsession id with leading ';'");
        normalizeTest(  "http://www.foo.com/foo.php?x=1&sid=xyz&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove sid session id in middle of valid query parameters");
        normalizeTest(  "http://www.foo.com/foo.php?x=1&-session=xyz&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove -sessionid session id in middle of valid query parameters");
    }
    
    @Test
    public void testOtherIgnoredQueryParametersRemoval() {
        normalizeTest(  "http://www.foo.com/foo.php?country=usa",
                        "http://www.foo.com/foo.php",
                        "Remove single country query parameter");
        normalizeTest(  "http://www.foo.com/foo.php?format=xyz&x=1&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove format before valid query parameters");
        normalizeTest(  "http://www.foo.com/foo.php?x=1&width=7&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove width in middle of valid query parameters");
        normalizeTest(  "http://www.foo.com/foo.php?x=1&something=1&height=7",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove height after valid query parameters");
    }
    
    @Test
    public void testAggressive() {
        normalizeTest(  "http://www.foo.com/foo.php?x=1&user_id=7&something=1",
                        "http://www.foo.com/foo.php?x=1&user_id=7&something=1",
                        "Leave user_id in middle of valid query parameters");
        SimpleUrlNormalizer aggresiveNormalizer = new SimpleUrlNormalizer(false, true);
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.php?user=usa",
                        "http://www.foo.com/foo.php",
                        "Remove single user query parameter");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.php?usr=xyz&x=1&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove usr before valid query parameters");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.php?x=1&user_id=7&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove user_id in middle of valid query parameters");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.php?x=1&something=1&userid=7",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove userid after valid query parameters");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.html?param=1&another=2;jsessionid=1E6FEC0D14D044541DD84D2D013D29ED",
                        "http://www.foo.com/foo.html?param=1&another=2",
                        "Remove jsession id with leading ';' after valid query parameters");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.html;jsessionid=1E6FEC0D14D044541DD84D2D013D29ED?param=1&another=2",
                        "http://www.foo.com/foo.html?param=1&another=2",
                        "Remove jsession id with leading ';'");
        normalizeTest(  aggresiveNormalizer,
                        "http://www.foo.com/foo.php?x=1&width=7&something=1",
                        "http://www.foo.com/foo.php?x=1&something=1",
                        "Remove width in middle of valid query parameters");
    }
    
    @Test
    public void testDefaultPageRemoval() {
        normalizeTest(  "http://www.foo.com/home/index.html",
                        "http://www.foo.com/home/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.html",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.htm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.asp",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.aspx",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.php",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.php3",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.html",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.htm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.asp",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.aspx",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.php",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.php3",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/something.php3",
                        "http://www.foo.com/something.php3",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/something.html",
                        "http://www.foo.com/something.html",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/something.asp",
                        "http://www.foo.com/something.asp",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.phtml",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.cfm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.cgi",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.HTML",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.Htm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.ASP",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jsp",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jsf",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jspx",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jspfx",
                        "http://www.foo.com/index.jspfx",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jspa",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.jsps",
                        "http://www.foo.com/index.jsps",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.aspX",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.PhP",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.PhP4",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.HTml",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.HTm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.ASp",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.AspX",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.PHP",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/default.PHP3",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.phtml",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.cfm",
                        "http://www.foo.com/",
                        "Remove default page");
        normalizeTest(  "http://www.foo.com/index.cgi",
                        "http://www.foo.com/",
                        "Remove default page");
    }
    
    @Test
    public void testStumbleUponURLs() {
    	SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer(true);

        normalizeTest(normalizer, "http://www.stumbleupon.com/toolbar/#url=http%3A//links.flashdance.cx/misc-pix/fjortisfangelse.jpg",
                        "http://www.stumbleupon.com/toolbar/#url=http%3a//links.flashdance.cx/misc-pix/fjortisfangelse.jpg",
        "Preserve pseudo-anchor used as qeury");

        normalizeTest(normalizer, "http://www.stumbleupon.com/toolbar/#topic=Poetry&url=http%3A//www.notellmotel.org/poem_single.php%3Fid%3D78_0_1_0",
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
    public void testDotAtEndOfDomainName() {
        normalizeTest("http://www.domain.com.", "http://www.domain.com/", "Remove trailing dot");
        normalizeTest("http://www.domain.com.:8080", "http://www.domain.com:8080/", "Remove trailing dot (with port)");
        normalizeTest("http://www.domain.com./path/page.html", "http://www.domain.com/path/page.html", "Remove trailing dot (with path)");
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
