/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.bixolabs.cascading.HadoopUtils;

@SuppressWarnings("deprecation")
public class DemoWebMiningWorkflowTest {

    private static final String WORKING_DIR = "build/test/WebMiningWorkflowTest";

    private static final String PAGE1_URL = "http://127.0.0.1:8089/page-1.html";
    private static final String PAGE1_SCORE = "0.021978023";
    private static final String PAGE2_URL = "http://127.0.0.1:8089/page-2.html";
    private static final String PAGE2_SCORE = "0.0";

    /**
     * Handler we use with embedded Jetty to "serve" web pages that are files found in a base
     * directory.
     *
     */
    private class DirectoryResponseHandler extends AbstractHandler {

        private File _baseDir;

        public DirectoryResponseHandler (String baseDir) {
            _baseDir = new File(baseDir);
            if (!_baseDir.exists() || !_baseDir.isDirectory()) {
                throw new RuntimeException("Base dir doesn't exist:" + _baseDir.getAbsolutePath());
            }
        }
        
        @Override
        public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
            
            File file = new File(_baseDir, pathInContext);
            if (!file.exists()) {
                throw new HttpException(404, "Resource not found: " + pathInContext);
            }

            try {
                byte[] bytes = new byte[(int) file.length()];
                DataInputStream in = new DataInputStream(new FileInputStream(file));
                in.readFully(bytes);
                
                response.setContentLength(bytes.length);
                response.setContentType("text/html");
                response.setStatus(200);
                
                OutputStream os = response.getOutputStream();
                os.write(bytes);
            } catch (Exception e) {
                throw new HttpException(500, e.getMessage());
            }
        }
    }

    private File _workingDir;
    
    @Before
    public void setup() {
        _workingDir = new File(WORKING_DIR);
        if (_workingDir.exists()) {
            FileUtils.deleteQuietly(_workingDir);
        }
        
        _workingDir.mkdirs();
    }
    
    @Test
    public void testDemoWebMiningWorkflow() throws Exception {
        DemoWebMiningOptions options = new DemoWebMiningOptions();
        options.setWorkingDir(WORKING_DIR);
        options.setAgentName("test-agent");
        Path workingDirPath = new Path(WORKING_DIR);
        FileSystem fs = workingDirPath.getFileSystem(new JobConf());
        DemoWebMiningTool.setupWorkingDir(fs, workingDirPath, "/test-seedurls.txt");
        
        Path latestDirPath = CrawlDirUtils.findLatestLoopDir(fs, workingDirPath);
        Path crawlDbPath = new Path(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        
        FetcherPolicy fetcherPolicy = new FetcherPolicy();
        fetcherPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
        fetcherPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
        fetcherPolicy.setFetcherMode(FetcherMode.EFFICIENT);
        Set<String> validMimeTypes = new HashSet<String>();
        validMimeTypes.add("text/plain");
        validMimeTypes.add("text/html");
        fetcherPolicy.setValidMimeTypes(validMimeTypes);

        UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);

        Server server = null;
        try {
            server = startServer(new DirectoryResponseHandler("src/test/resources/test-pages"), 8089);
            
            Path curLoopDirPath = CrawlDirUtils.makeLoopDir(fs, workingDirPath, 1);

            Flow flow = DemoWebMiningWorkflow.createWebMiningWorkflow(crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options);
            flow.complete();
        
            // validate
            Path statusPath = new Path(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
            validateEntryCount(statusPath, null, 1, "status", true);
    
            Path contentPath = new Path(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
            validateEntryCount(contentPath, FetchedDatum.FIELDS, 1, "content", false);

            crawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            validateEntryCount(crawlDbPath, null, 3, "crawldb", true);
            
            // run the second loop
            curLoopDirPath =  CrawlDirUtils.makeLoopDir(fs, workingDirPath, 2);
            flow = DemoWebMiningWorkflow.createWebMiningWorkflow(crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options);
            flow.complete();
            
            // validate
            statusPath = new Path(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
            validateEntryCount(statusPath, null, 2, "status", true);
    
            contentPath = new Path(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
            validateEntryCount(contentPath, FetchedDatum.FIELDS, 2, "content", false);

            crawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            validateEntryCount(crawlDbPath, null, 8, "crawldb", true);
            assertTrue(validatePageScores(crawlDbPath));
            
            Path resultsPath = new Path(curLoopDirPath, CrawlConfig.RESULTS_SUBDIR_NAME);
            validateEntryCount(resultsPath, null, 3, "page results", true);
        }  finally {
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

    private void validateEntryCount(Path dataPath, Fields fields, int expected, String msgStr, boolean isTextLine) throws IOException, InterruptedException {
        Hfs sourceTap;
        
        if (isTextLine) {
            sourceTap = new Hfs(new TextLine(), dataPath.toString(), false);
        } else {
            sourceTap = new Hfs(new SequenceFile(fields), dataPath.toString(), false);
        }
        
        TupleEntryIterator tupleEntryIterator = sourceTap.openForRead(HadoopUtils.getDefaultJobConf());
        int numEntries = 0;
        while (tupleEntryIterator.hasNext()) {
          tupleEntryIterator.next();
          numEntries++;
        }
        
        tupleEntryIterator.close();
        assertEquals(msgStr, expected, numEntries);
    }

    private boolean validatePageScores(Path dataPath) throws IOException, InterruptedException {
        boolean allOK = false;
        int verifiedCnt = 0;
        Hfs sourceTap = new Hfs(new TextLine(), dataPath.toString(), false);
        TupleEntryIterator tupleEntryIterator = sourceTap.openForRead(HadoopUtils.getDefaultJobConf());
        while (tupleEntryIterator.hasNext()) {
          TupleEntry next = tupleEntryIterator.next();
          String line = next.getString("line");
          String[] split = line.split("\t");
          if (split[0].equals(PAGE1_URL)) {
              allOK |= split[3].equals(PAGE1_SCORE);
              verifiedCnt++;
          } else if (split[0].equals(PAGE2_URL)) {
              allOK |= split[3].equals(PAGE2_SCORE);
              verifiedCnt++;
          }
        }
        
        tupleEntryIterator.close();
        return allOK && (verifiedCnt==2);
     }
}
