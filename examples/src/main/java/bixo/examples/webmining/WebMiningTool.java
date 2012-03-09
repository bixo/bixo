/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;

import com.bixolabs.cascading.HadoopUtils;

@SuppressWarnings("deprecation")
public class WebMiningTool {

    private static final Logger LOGGER = Logger.getLogger(WebMiningTool.class);

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    static void setupWorkingDir(FileSystem fs, Path workingDirPath, String seedUrlsfileName) throws Exception {
        
        // Check if we already have a crawldb
        Path crawlDbPath = null;
        Path loopDirPath = CrawlDirUtils.findLatestLoopDir(fs, workingDirPath);
        if (loopDirPath != null) {
            // Clear out any previous loop directory, so we're always starting from scratch
            LOGGER.info("deleting existing working dir");
            while (loopDirPath != null) {
                fs.delete(loopDirPath, true);
                loopDirPath = CrawlDirUtils.findLatestLoopDir(fs, workingDirPath);
            }
        } 

        // Create a "0-<timestamp>" loop sub-directory and import the seed urls
        loopDirPath = CrawlDirUtils.makeLoopDir(fs, workingDirPath, 0);
        crawlDbPath = new Path(loopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
        WebMiningWorkflow.importSeedUrls(crawlDbPath, seedUrlsfileName);


    }

    private static void error(String message, CmdLineParser parser) {
        System.err.println(message);
        printUsageAndExit(parser);
    }

    public static void main(String[] args) throws IOException {
        
        WebMiningOptions options = new WebMiningOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Build and run the flow.
        try {
            String username = options.getUsername();
            
            // If we're running on the real cluster, ignore the working dir and use a directory in S3
            Path workingDirPath;
            if (HadoopUtils.isJobLocal(new JobConf())) {
                workingDirPath = new Path(options.getWorkingDir());
            } else {
                workingDirPath = new Path("s3n://strata-web-mining-students/" + username + "/working/");
            }

            JobConf conf = new JobConf();
            FileSystem fs = workingDirPath.getFileSystem(conf);
            setupWorkingDir(fs, workingDirPath, CrawlConfig.SEED_URLS_FILENAME);
 
            Path latestDirPath = CrawlDirUtils.findLatestLoopDir(fs, workingDirPath);
            if (latestDirPath == null) {
                error("No previous cycle output dirs exist in " + workingDirPath, parser);
            }
            
            Path crawlDbPath = new Path(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            
            UserAgent userAgent = new UserAgent(CrawlConfig.SPIDER_NAME, CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);
            
            FetcherPolicy fetcherPolicy = new FetcherPolicy();
            fetcherPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
            fetcherPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
            fetcherPolicy.setFetcherMode(FetcherMode.EFFICIENT);
            // You can also provide a set of mime types you want to restrict what content type you 
            // want to deal with - for now keep it simple.
            Set<String> validMimeTypes = new HashSet<String>();
            validMimeTypes.add("text/plain");
            validMimeTypes.add("text/html");
            fetcherPolicy.setValidMimeTypes(validMimeTypes);

            // Let's limit our crawl to two loops 
            for (int curLoop = 1; curLoop <= 2; curLoop++) {
                Path curLoopDirPath = CrawlDirUtils.makeLoopDir(fs, workingDirPath, curLoop);
                Flow flow = WebMiningWorkflow.createFetchWorkflow(crawlDbPath, curLoopDirPath, fetcherPolicy, userAgent, options, curLoop == 1);
                flow.complete();

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
            
        } catch (Exception e) {
            System.err.println("Exception running job: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
