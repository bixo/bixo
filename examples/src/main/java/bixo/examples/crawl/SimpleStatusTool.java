/*
 * Copyright (c) 2010 TransPac Software, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.examples.crawl;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.datum.UrlStatus;
import bixo.utils.CrawlDirUtils;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings("deprecation")
public class SimpleStatusTool {
	private static final Logger LOGGER = Logger.getLogger(SimpleStatusTool.class);
	
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

	private static void processStatus(JobConf conf, Path curDirPath) throws IOException {
        Path statusPath = new Path(curDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
        Tap statusTap = new Hfs(new TextLine(), statusPath.toUri().toString());
        
        TupleEntryIterator iter = statusTap.openForRead(conf);
        
        UrlStatus[] statusValues = UrlStatus.values();
        int[] statusCounts = new int[statusValues.length];
        int totalEntries = 0;
        while (iter.hasNext()) {
            TupleEntry entry = iter.next();
            totalEntries += 1;
    
            // STATUS_FN, HEADERS_FN, EXCEPTION_FN, STATUS_TIME_FN, HOST_ADDRESS_FN).append(getSuperFields(StatusDatum.class)
            String statusLine = entry.getString("line");
            String[] pieces = statusLine.split("\t");
            UrlStatus status = UrlStatus.valueOf(pieces[0]);
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

    private static void processCrawlDb(JobConf conf, Path curDirPath, boolean exportDb) throws IOException {
        TupleEntryIterator iter;
        int totalEntries;
        Path crawlDbPath = new Path(curDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        Tap crawldbTap = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString());
        iter = crawldbTap.openForRead(conf);
        totalEntries = 0;
        int fetchedUrls = 0;
        int unfetchedUrls = 0;
        
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
        SimpleStatusToolOptions options = new SimpleStatusToolOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String crawlDirName = options.getCrawlDir();

        try {
        	JobConf conf = new JobConf();
        	Path crawlDirPath = new Path(crawlDirName);
        	FileSystem fs = crawlDirPath.getFileSystem(conf);

        	if (!fs.exists(crawlDirPath)) {
    			System.err.println("Prior crawl output directory does not exist: " + crawlDirName);
    			System.exit(-1);
        	}
        	
        	// Skip Hadoop/Cascading DEBUG messages.
        	Logger.getRootLogger().setLevel(Level.INFO);
        	
        	boolean exportDb = options.isExportDb();
        	if (exportDb) {
        	    Path latestCrawlDirPath = CrawlDirUtils.findLatestLoopDir(fs, crawlDirPath);
        	    processCrawlDb(conf, latestCrawlDirPath, exportDb);
        	} else {
            	int prevLoop = -1;
            	Path curDirPath = null;
            	while ((curDirPath = CrawlDirUtils.findNextLoopDir(fs, crawlDirPath, prevLoop)) != null) {
            		String curDirName = curDirPath.toUri().toString();
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
            		processStatus(conf, curDirPath);
                    processCrawlDb(conf, curDirPath, exportDb);
                    
            	}
        	}
        } catch (Throwable t) {
        	LOGGER.error("Exception running tool", t);
            System.exit(-1);
        }
	}

}
