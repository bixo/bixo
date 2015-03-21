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
package bixo.examples.crawl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.HttpStatus;
import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.BixoPlatform.Platform;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlFilter;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;

@SuppressWarnings({ "serial", "deprecation" })
public class DemoCrawlWorkflowLRTest implements Serializable {

    public static class FakeWebSiteHandler extends AbstractHandler {

        private static final Pattern PAGE_PATTERN = Pattern.compile("/page-(\\d+)\\.html");

        private static final String HTML_TEMPLATE = "<html><head><title>Untitled</title></head><body>%s</body></html>";
        private static final String ANCHOR_TEMPLATE = "<a href=\"/page-%d.html\"></a>\n";

        @Override
        public void handle(String pathInContext,  Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            if (pathInContext.equals("/")) {
                response.sendRedirect("/page-1.html");
            } else {
                Matcher matcher = PAGE_PATTERN.matcher(pathInContext);
                if (!matcher.matches()) {
                    throw new HttpException(HttpStatus.SC_NOT_FOUND);
                }

                int curPage = Integer.parseInt(matcher.group(1));
                StringBuilder innerResult = new StringBuilder();
                for (int nextPage = 0; nextPage < 10; nextPage++) {
                    String nextAnchor = String.format(ANCHOR_TEMPLATE, (curPage * 10) + nextPage);
                    innerResult.append(nextAnchor);
                }

                String htmlContent = String.format(HTML_TEMPLATE, innerResult.toString());
                byte[] byteContent = htmlContent.getBytes("UTF-8");
                response.setContentLength(byteContent.length);
                response.setContentType("text/html; charset=UTF-8");
                response.setStatus(HttpStatus.SC_OK);
                response.getOutputStream().write(byteContent);
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testNotLosingFetchedUrls() throws Throwable {
        String baseDirName = "build/test/DemoCrawlWorkflowLRTest/output";
        
        BixoPlatform platform = new BixoPlatform(DemoCrawlWorkflowLRTest.class, Platform.Local);
        
        BasePath baseDirPath = platform.makePath(baseDirName);
        baseDirPath.delete(true);
        BasePath curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, baseDirPath, 0);
        BasePath crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

        DemoCrawlTool.importOneDomain(platform, "localhost:8089", crawlDbPath);
        curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, baseDirPath, 1);

        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlDelay(1);
        defaultPolicy.setFetcherMode(FetcherMode.COMPLETE);
        BaseUrlFilter urlFilter = new BaseUrlFilter() {

            @Override
            public boolean isRemove(UrlDatum datum) {
                return false;
            }
        };

        DemoCrawlToolOptions options = new DemoCrawlToolOptions();
        options.setUseBoilerpipe(true);
        options.setLocalPlatformMode(true);
        UserAgent userAgent = new UserAgent("test", "test@domain.com", "http://test.domain.com");
        Server server = null;
        try {
            server = startServer(new FakeWebSiteHandler(), 8089);
            Flow flow = DemoCrawlWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, options);
            flow.complete();

            // Update the crawlDb path
            crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

            // Now we should have an output/1-<timestamp>/ directory, where the
            // /urls dir has 11 entries with
            // one being previously crawled, and the other 10 being pending.

            Tap crawldbTap = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), crawlDbPath);
            TupleEntryIterator iter = crawldbTap.openForRead(platform.makeFlowProcess());

            int numFetched = 0;
            int numPending = 0;
            while (iter.hasNext()) {
                CrawlDbDatum datum = new CrawlDbDatum(iter.next());
                UrlStatus status = datum.getLastStatus();
                int crawlDepth = datum.getCrawlDepth();
                if (datum.getLastFetched() != 0) {
                    numFetched += 1;

                    assertEquals(UrlStatus.FETCHED, status);
                    assertEquals(0, crawlDepth);
                } else {
                    numPending += 1;
                    assertEquals(UrlStatus.UNFETCHED, status);
                    assertEquals(1, crawlDepth);
                }
            }

            assertEquals(1, numFetched);
            assertEquals(10, numPending);

            // Do it one more time, to verify status gets propagated forward.
            curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, baseDirPath, 2);

            flow = DemoCrawlWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, options);
            flow.complete();
            // Update crawldb path
            crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

            crawldbTap = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), crawlDbPath);
            iter = crawldbTap.openForRead(platform.makeFlowProcess());

            numFetched = 0;
            numPending = 0;
            int numDepth0 = 0;
            int numDepth1 = 0;
            int numDepth2 = 0;
            while (iter.hasNext()) {
                CrawlDbDatum datum = new CrawlDbDatum(iter.next());
                UrlStatus status = datum.getLastStatus();
                int depth = datum.getCrawlDepth();

                if (datum.getLastFetched() != 0) {
                    numFetched += 1;
                    assertEquals("URL has incorrect status: " + datum.getUrl(), UrlStatus.FETCHED, status);
                } else {
                    numPending += 1;
                    assertEquals("URL has incorrect status: " + datum.getUrl(), UrlStatus.UNFETCHED, status);
                }

                if (depth == 0) {
                    numDepth0 += 1;
                } else if (depth == 1) {
                    numDepth1 += 1;
                } else if (depth == 2) {
                    numDepth2 += 1;
                } else {
                    fail("Invalid crawl depth for " + datum.getUrl());
                }

                // System.out.println(String.format("URL %s has status %s, last fetch %d, and depth %d",
                // datum.getUrl(), datum.getLastStatus(),
                // datum.getLastFetched(), depth));
            }

            assertEquals(11, numFetched);
            assertEquals(100, numPending);

            assertEquals(1, numDepth0);
            assertEquals(10, numDepth1);
            assertEquals(100, numDepth2);
        } catch (Throwable t) {
            fail(t.getMessage());
        } finally {
            if (server != null) {
                server.stop();
            }
        }

    }

    private Server startServer(Handler handler, int port) throws Exception {
        Server server = new Server(port);
        server.setHandler(handler);
        server.start();
        return server;
    }

}
