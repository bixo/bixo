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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

@SuppressWarnings("deprecation")
public class DemoWebMiningTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoWebMiningTool.class);

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    static void setupWorkingDir(BasePlatform platform, BasePath workingDirPath, String seedUrlsfileName) throws Exception {
        
        // Check if we already have a crawldb
        BasePath crawlDbPath = null;
        BasePath loopDirPath = CrawlDirUtils.findLatestLoopDir(platform, workingDirPath);
        if (loopDirPath != null) {
            // Clear out any previous loop directory, so we're always starting from scratch
            LOGGER.info("deleting existing working dir");
            while (loopDirPath != null) {
                loopDirPath.delete(true);
                loopDirPath = CrawlDirUtils.findLatestLoopDir(platform, workingDirPath);
            }
        } 

        // Create a "0-<timestamp>" loop sub-directory and import the seed urls
        loopDirPath = CrawlDirUtils.makeLoopDir(platform, workingDirPath, 0);
        crawlDbPath = platform.makePath(loopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        DemoWebMiningWorkflow.importSeedUrls(platform, crawlDbPath, seedUrlsfileName);


    }

    private static void error(String message, CmdLineParser parser) {
        System.err.println(message);
        printUsageAndExit(parser);
    }

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws IOException {
        
        DemoWebMiningOptions options = new DemoWebMiningOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Build and run the flow.
        
        try {
            BixoPlatform platform = new BixoPlatform(DemoWebMiningTool.class, options.getPlatformMode());
            BasePath workingDirPath = platform.makePath(options.getWorkingDir());

            setupWorkingDir(platform, workingDirPath, CrawlConfig.SEED_URLS_FILENAME);
 
            BasePath latestDirPath = CrawlDirUtils.findLatestLoopDir(platform, workingDirPath);
            if (latestDirPath == null) {
                error("No previous cycle output dirs exist in " + workingDirPath, parser);
            }
            
            BasePath crawlDbPath = platform.makePath(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            
            UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);
            
            FetcherPolicy fetcherPolicy = new FetcherPolicy();
            fetcherPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
            fetcherPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
            fetcherPolicy.setFetcherMode(FetcherMode.EFFICIENT);
            
            // We only care about mime types that the Tika HTML parser can handle,
            // so restrict it to the same.
            Set<String> validMimeTypes = new HashSet<String>();
            Set<MediaType> supportedTypes = new HtmlParser().getSupportedTypes(new ParseContext());
            for (MediaType supportedType : supportedTypes) {
                validMimeTypes.add(String.format("%s/%s", supportedType.getType(), supportedType.getSubtype()));
            }
            fetcherPolicy.setValidMimeTypes(validMimeTypes);

            // Let's limit our crawl to two loops 
            for (int curLoop = 1; curLoop <= 2; curLoop++) {
                BasePath curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, workingDirPath, curLoop);
                Flow flow = DemoWebMiningWorkflow.createWebMiningWorkflow(platform, crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options);
                flow.complete();

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
            
        } catch (Exception e) {
            System.err.println("Exception running job: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
