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

import org.apache.log4j.Level;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.datum.StatusDatum;
import bixo.datum.UrlStatus;
import bixo.utils.CrawlDirUtils;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

public class DemoStatusTool {
	private static final Logger LOGGER = LoggerFactory.getLogger(DemoStatusTool.class);
	
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
    private static void processStatus(BasePlatform platform, BasePath curDirPath) throws Exception {
        BasePath statusPath = platform.makePath(curDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusTap = platform.makeTap(platform.makeTextScheme(), statusPath);
        
        TupleEntryIterator iter = statusTap.openForRead(platform.makeFlowProcess());
        
        LOGGER.info("Analyzing: " +  CrawlConfig.STATUS_SUBDIR_NAME);
        UrlStatus[] statusValues = UrlStatus.values();
        int[] statusCounts = new int[statusValues.length];
        int totalEntries = 0;
        while (iter.hasNext()) {
            TupleEntry entry = iter.next();
            totalEntries += 1;
    
            String statusLine = entry.getString("line");
            String[] pieces = statusLine.split("\t");
            int pos = StatusDatum.FIELDS.getPos(StatusDatum.STATUS_FN);
            UrlStatus status = UrlStatus.valueOf(pieces[pos]);
            statusCounts[status.ordinal()] += 1;
        }
        
        
        for (int i = 0; i < statusCounts.length; i++) {
        	if (statusCounts[i] != 0) {
        		LOGGER.info(String.format("Status %s: %d", statusValues[i].toString(), statusCounts[i]));
        	}
        }
        LOGGER.info("Total status: " + totalEntries);
        LOGGER.info("");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void processCrawlDb(BixoPlatform platform, BasePath latestCrawlDirPath, boolean exportDb) throws Exception {
        TupleEntryIterator iter;
        int totalEntries;
        BasePath crawlDbPath = platform.makePath(latestCrawlDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap crawldbTap = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), crawlDbPath);
        iter = crawldbTap.openForRead(platform.makeFlowProcess());
        totalEntries = 0;
        int fetchedUrls = 0;
        int unfetchedUrls = 0;
 
        LOGGER.info("Analyzing: " +  CrawlConfig.CRAWLDB_SUBDIR_NAME);

        while (iter.hasNext()) {
            TupleEntry entry = iter.next();
            totalEntries += 1;
            
            CrawlDbDatum datum = new CrawlDbDatum(entry);
            if (exportDb) {
                LOGGER.info(datum.toString());
            }
            if (datum.getLastFetched() == 0) {
            	unfetchedUrls += 1;
            } else {
            	fetchedUrls += 1;
            }
        }
        if (!exportDb) {
            LOGGER.info(String.format("%d fetched URLs", fetchedUrls));
            LOGGER.info(String.format("%d unfetched URLs", unfetchedUrls));
            LOGGER.info("Total URLs: " + totalEntries);
            LOGGER.info("");
        }
    }

    public static void main(String[] args) {
        DemoStatusToolOptions options = new DemoStatusToolOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String crawlDirName = options.getWorkingDir();

        try {
            BixoPlatform platform = new BixoPlatform(DemoStatusTool.class, options.getPlatformMode());
        	BasePath crawlDirPath = platform.makePath(crawlDirName);

        	crawlDirPath.assertExists("Prior crawl output directory does not exist");
        	
        	// Skip Hadoop/Cascading DEBUG messages.
            org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
        	
        	boolean exportDb = options.isExportDb();
        	if (exportDb) {
        	    BasePath latestCrawlDirPath = CrawlDirUtils.findLatestLoopDir(platform, crawlDirPath);
        	    processCrawlDb(platform, latestCrawlDirPath, exportDb);
        	} else {
            	int prevLoop = -1;
            	BasePath curDirPath = null;
            	while ((curDirPath = CrawlDirUtils.findNextLoopDir(platform, crawlDirPath, prevLoop)) != null) {
            		String curDirName = curDirPath.getAbsolutePath();
            		LOGGER.info("");
            		LOGGER.info("================================================================");
            		LOGGER.info("Processing " + curDirName);
            		LOGGER.info("================================================================");
            		
            		int curLoop = CrawlDirUtils.extractLoopNumber(curDirPath);
            		if (curLoop != prevLoop + 1) {
            			LOGGER.warn(String.format("Missing directories between %d and %d", prevLoop, curLoop));
            		}
            		
            		prevLoop = curLoop;
            		
            		// Process the status and crawldb in curPath
            		processStatus(platform, curDirPath);
                    processCrawlDb(platform, curDirPath, exportDb);
                    
            	}
        	}
        } catch (Throwable t) {
        	LOGGER.error("Exception running tool", t);
            System.exit(-1);
        }
	}

}
