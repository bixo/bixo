package bixo.tools.sitecrawler;

import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.handler.AbstractHttpHandler;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.urldb.IUrlFilter;
import bixo.utils.FSUtils;
import bixo.utils.HadoopUtils;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tuple.TupleEntryIterator;


@SuppressWarnings("serial")
public class SiteCrawlerLRTest implements Serializable {

    public static class FakeWebSiteHandler extends AbstractHttpHandler {

        private static final Pattern PAGE_PATTERN = Pattern.compile("/page-(\\d+)\\.html");
        
        private static final String HTML_TEMPLATE = "<html><head><title>Untitled</title></head><body>%s</body></html>";
        private static final String ANCHOR_TEMPLATE = "<a href=\"/page-%d.html\"></a>\n";
        
        @Override
        public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response)
                        throws HttpException, IOException {
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
    
    @Test
    public void testNotLosingFetchedUrls() throws Throwable {
        String baseDirName = "build/test/SiteCrawlerTest/output";
        JobConf conf = new JobConf();
        Path baseDirPath = new Path(baseDirName);
        FileSystem fs = baseDirPath.getFileSystem(conf);

        HadoopUtils.safeRemove(fs, baseDirPath);
        Path outputPath = FSUtils.makeLoopDir(fs, baseDirPath, 0);
        UrlImporter importer = new UrlImporter(outputPath);

        importer.importOneDomain("localhost:8089", false);
        Path inputPath = outputPath;
        outputPath = FSUtils.makeLoopDir(fs, baseDirPath, 1);

        FetcherPolicy defaultPolicy = new FetcherPolicy();
        defaultPolicy.setCrawlDelay(1);

        IUrlFilter urlFilter = new IUrlFilter() {

            @Override
            public boolean isRemove(UrlDatum datum) {
                return false;
            }
        };

        SiteCrawlerServer server = new SiteCrawlerServer(new FakeWebSiteHandler(), 8089);

        try {
        	UserAgent userAgent = new UserAgent("test", "test@domain.com", "http://test.domain.com");
            SiteCrawler crawler = new SiteCrawler(inputPath, outputPath, userAgent, defaultPolicy, 1, urlFilter);
            crawler.crawl(false);
        } catch (Throwable t) {
        	t.printStackTrace();
            Assert.fail(t.getMessage());
        } finally {
            server.stop();
        }

        // Now we should have an output/1-<timestamp>/ directory, where the /urls dir has 11 entries with
        // one being previously crawled, and the other 10 being pending.
        String dirName = outputPath.toUri().toString();
        Hfs statusTap = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), dirName + "/urls");
        TupleEntryIterator iter = statusTap.openForRead(conf);

        int numFetched = 0;
        int numPending = 0;
        while (iter.hasNext()) {
            UrlDatum datum = new UrlDatum(iter.next().getTuple(), MetaData.FIELDS);

            if (datum.getLastFetched() != 0) {
                numFetched += 1;
                Assert.assertEquals(UrlStatus.FETCHED, datum.getLastStatus());
                Assert.assertEquals("0", (String)datum.getMetaDataValue("crawl-depth"));
            } else {
                numPending += 1;
                Assert.assertEquals(UrlStatus.UNFETCHED, datum.getLastStatus());
                Assert.assertEquals("1", (String)datum.getMetaDataValue("crawl-depth"));
            }
        }

        Assert.assertEquals(1, numFetched);
        Assert.assertEquals(10, numPending);
        
        // Do it one more time, to verify status gets propagated forward.
        inputPath = outputPath;
        outputPath = FSUtils.makeLoopDir(fs, baseDirPath, 2);

        server = new SiteCrawlerServer(new FakeWebSiteHandler(), 8089);

        try {
        	UserAgent userAgent = new UserAgent("test", "test@domain.com", "http://test.domain.com");
            SiteCrawler crawler = new SiteCrawler(inputPath, outputPath, userAgent, defaultPolicy, 1, urlFilter);
            crawler.crawl(false);
        } catch (Throwable t) {
            Assert.fail(t.getMessage());
        } finally {
            server.stop();
        }
        
        dirName = outputPath.toUri().toString();
        statusTap = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), dirName + "/urls");
        iter = statusTap.openForRead(conf);

        numFetched = 0;
        numPending = 0;
        int numDepth0 = 0;
        int numDepth1 = 0;
        int numDepth2 = 0;
        while (iter.hasNext()) {
            UrlDatum datum = new UrlDatum(iter.next().getTuple(), MetaData.FIELDS);

            if (datum.getLastFetched() != 0) {
                numFetched += 1;
                Assert.assertEquals("URL has incorrect status: " + datum.getUrl(), UrlStatus.FETCHED, datum.getLastStatus());
            } else {
                numPending += 1;
                Assert.assertEquals("URL has incorrect status: " + datum.getUrl(), UrlStatus.UNFETCHED, datum.getLastStatus());
            }
            
            int depth = Integer.parseInt((String)datum.getMetaDataValue("crawl-depth"));
            if (depth == 0) {
                numDepth0 += 1;
            } else if (depth == 1) {
                numDepth1 += 1;
            } else if (depth == 2) {
                numDepth2 += 1;
            } else {
                Assert.fail("Invalid crawl depth for " + datum.getUrl());
            }
            
            // System.out.println(String.format("URL %s has status %s, last fetch %d, and depth %d", datum.getUrl(), datum.getLastStatus(), datum.getLastFetched(), depth));
        }

        Assert.assertEquals(11, numFetched);
        Assert.assertEquals(100, numPending);
        
        Assert.assertEquals(1, numDepth0);
        Assert.assertEquals(10, numDepth1);
        Assert.assertEquals(100, numDepth2);
    }
}
