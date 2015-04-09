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
import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Before;
import org.junit.Test;

import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;


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
        public void handle(String pathInContext, Request baseReques, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            
            File file = new File(_baseDir, pathInContext);
            if (!file.exists()) {
                throw new HttpException(404, "Resource not found: " + pathInContext);
            }

            try {
                byte[] bytes = new byte[(int) file.length()];
                DataInputStream in = new DataInputStream(new FileInputStream(file));
                in.readFully(bytes);
                in.close();
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
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testDemoWebMiningWorkflow() throws Exception {
        DemoWebMiningOptions options = new DemoWebMiningOptions();
        options.setWorkingDir(WORKING_DIR);
        options.setAgentName("test-agent");
        options.setLocalPlatformMode(true);
        
        BixoPlatform platform = new BixoPlatform(DemoWebMiningWorkflowTest.class, options.getPlatformMode());
        BasePath workingDirPath = platform.makePath(WORKING_DIR);
        DemoWebMiningTool.setupWorkingDir(platform, workingDirPath, "/test-seedurls.txt");
        
        BasePath latestDirPath = CrawlDirUtils.findLatestLoopDir(platform, workingDirPath);
        BasePath crawlDbPath = platform.makePath(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        
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
            
            BasePath curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, workingDirPath, 1);

            Flow flow = DemoWebMiningWorkflow.createWebMiningWorkflow(platform, crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options);
            flow.complete();
        
            // validate
            BasePath statusPath = platform.makePath(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
            validateEntryCount(platform, statusPath, null, 1, "status", true);
    
            BasePath contentPath = platform.makePath(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
            validateEntryCount(platform, contentPath, FetchedDatum.FIELDS, 1, "content", false);

            crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            validateEntryCount(platform, crawlDbPath, null, 3, "crawldb", true);
            
            // run the second loop
            curLoopDirPath =  CrawlDirUtils.makeLoopDir(platform, workingDirPath, 2);
            flow = DemoWebMiningWorkflow.createWebMiningWorkflow(platform, crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options);
            flow.complete();
            
            // validate
            statusPath = platform.makePath(curLoopDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
            validateEntryCount(platform, statusPath, null, 2, "status", true);
    
            contentPath = platform.makePath(curLoopDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
            validateEntryCount(platform, contentPath, FetchedDatum.FIELDS, 2, "content", false);

            crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            validateEntryCount(platform, crawlDbPath, null, 8, "crawldb", true);
            assertTrue(validatePageScores(platform, crawlDbPath));
            
            BasePath resultsPath = platform.makePath(curLoopDirPath, CrawlConfig.RESULTS_SUBDIR_NAME);
            validateEntryCount(platform, resultsPath, null, 3, "page results", true);
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void validateEntryCount(BasePlatform platform, BasePath dataPath, Fields fields, int expected, String msgStr, boolean isTextLine) throws Exception {
        Tap sourceTap;
        
        if (isTextLine) {
            sourceTap = platform.makeTap(platform.makeTextScheme(), dataPath);
        } else {
            sourceTap = platform.makeTap(platform.makeBinaryScheme(fields), dataPath);
        }
        
        TupleEntryIterator tupleEntryIterator = sourceTap.openForRead(platform.makeFlowProcess());
        int numEntries = 0;
        while (tupleEntryIterator.hasNext()) {
          tupleEntryIterator.next();
          numEntries++;
        }
        
        tupleEntryIterator.close();
        assertEquals(msgStr, expected, numEntries);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean validatePageScores(BasePlatform platform, BasePath  dataPath) throws Exception {
        boolean allOK = false;
        int verifiedCnt = 0;
        Tap sourceTap = platform.makeTap(platform.makeTextScheme(), dataPath);
        TupleEntryIterator tupleEntryIterator = sourceTap.openForRead(platform.makeFlowProcess());
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
