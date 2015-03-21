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

import java.util.List;

import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bixo.config.BixoPlatform;
import bixo.config.FetcherPolicy;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.config.UserAgent;
import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;


@SuppressWarnings("deprecation")
public class DemoCrawlTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoCrawlTool.class);
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    // Create log output file (in the local file system).
    private static void setLoopLoggerFile(String outputDirName, int loopNumber) {

        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        String filename = String.format("%s/%d-DemoCrawlTool.log", outputDirName, loopNumber);
        FileAppender appender = (FileAppender)rootLogger.getAppender("loop-logger");
        if (appender == null) {
            appender = new FileAppender();
            appender.setName("loop-logger");
            appender.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}:%L - %m%n"));
            
            // We have to do this before calling addAppender, as otherwise Log4J warns us.
            appender.setFile(filename);
            appender.activateOptions();
            rootLogger.addAppender(appender);
        } else {
            appender.setFile(filename);
            appender.activateOptions();
        }
    }
    
    private static void validateDomain(String domain, CmdLineParser parser) {
        if (domain.startsWith("http")) {
            System.err.println("The target domain should be specified as just the host, without the http protocol: " + domain);
            printUsageAndExit(parser);
        }
        
        if (!domain.equals("localhost") && (domain.split("\\.").length < 2)) {
            System.err.println("The target domain should be a valid paid-level domain or subdomain of the same: " + domain);
            printUsageAndExit(parser);
        }
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void importOneDomain(BasePlatform platform, String targetDomain, BasePath crawlDbPath) throws Exception {
        
        try {
            Tap urlSink = platform.makeTap(platform.makeBinaryScheme(CrawlDbDatum.FIELDS), crawlDbPath, SinkMode.REPLACE);
            TupleEntryCollector writer = urlSink.openForWrite(platform.makeFlowProcess());
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

            CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0);

            writer.add(datum.getTuple());
            writer.close();
        } catch (Exception e) {
            throw e;
        }
    }


    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        DemoCrawlToolOptions options = new DemoCrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Before we get too far along, see if the domain looks valid.
        String domain = options.getDomain();
        String urlsFile = options.getUrlsFile();
        if (domain != null) {
            validateDomain(domain, parser);
        } else {
            if (urlsFile == null) {
                System.err.println("Either a target domain should be specified or a file with a list of urls needs to be provided");
                printUsageAndExit(parser);
            }
        }
        
        if (domain != null && urlsFile != null) {
            System.out.println("Warning: Both domain and urls file list provided - using domain");
        }
        
        String outputDirName = options.getOutputDir();
        if (options.isDebugLogging()) {
            System.setProperty("bixo.root.level", "DEBUG");
        } else {
            System.setProperty("bixo.root.level", "INFO");
        }
        
        if (options.getLoggingAppender() != null) {
            // Set console vs. DRFA vs. something else
            System.setProperty("bixo.appender", options.getLoggingAppender());
        }
        
        String logsDir = options.getLogsDir();
        if (!logsDir.endsWith("/")) {
            logsDir = logsDir + "/";
        }
        
        try {
            BixoPlatform platform = new BixoPlatform(DemoCrawlTool.class, options.getPlatformMode());
            BasePath outputPath = platform.makePath(outputDirName);

            // First check if the user want to clean
            if (options.isCleanOutputDir()) {
                if (outputPath.exists()) {
                    outputPath.delete(true);
                }
            }
            
            // See if the user isn't starting from scratch then set up the 
            // output directory and create an initial urls subdir.
            if (!outputPath.exists()) {
                outputPath.mkdirs();

                // Create a "0-<timestamp>" sub-directory with just a /crawldb subdir
                // In the /crawldb dir the input file will have a single URL for the target domain.

                BasePath curLoopDir = CrawlDirUtils.makeLoopDir(platform, outputPath, 0);
                String curLoopDirName = curLoopDir.getName();
                setLoopLoggerFile(logsDir + curLoopDirName, 0);
                BasePath crawlDbPath = platform.makePath(curLoopDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);
                
                if (domain != null) {
                    importOneDomain(platform, domain, crawlDbPath);
                } else {
                    BasePath urlsPath = platform.makePath(urlsFile);
                    UrlImporter urlImporter = new UrlImporter(platform, urlsPath, crawlDbPath);
                    urlImporter.importUrls(false);                }
            }
            
            BasePath latestDirPath = CrawlDirUtils.findLatestLoopDir(platform, outputPath);

            if (latestDirPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                printUsageAndExit(parser);
            }

            BasePath crawlDbPath = platform.makePath(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

            // Set up the start and end loop counts.
            int startLoop = CrawlDirUtils.extractLoopNumber(latestDirPath);
            int endLoop = startLoop + options.getNumLoops();

            // Set up the UserAgent for the fetcher.
            UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);

            // You also get to customize the FetcherPolicy
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
            defaultPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
            defaultPolicy.setFetcherMode(FetcherMode.EFFICIENT);
            
            // It is a good idea to set up a crawl duration when running long crawls as you may 
            // end up in situations where the fetch slows down due to a 'long tail' and by 
            // specifying a crawl duration you know exactly when the crawl will end.
            int crawlDurationInMinutes = options.getCrawlDuration();
            boolean hasEndTime = crawlDurationInMinutes != DemoCrawlToolOptions.NO_CRAWL_DURATION;
            long targetEndTime = hasEndTime ? System.currentTimeMillis() + (crawlDurationInMinutes * CrawlConfig.MILLISECONDS_PER_MINUTE) : 
                FetcherPolicy.NO_CRAWL_END_TIME;

            // By setting up a url filter we only deal with urls that we want to
            // instead of all the urls that we extract.
            BaseUrlFilter urlFilter = null;
            List<String> patterns = null;
            String regexUrlFiltersFile = options.getRegexUrlFiltersFile();
            if (regexUrlFiltersFile != null) {
                patterns = RegexUrlFilter.getUrlFilterPatterns(regexUrlFiltersFile);
            } else {
                patterns = RegexUrlFilter.getDefaultUrlFilterPatterns();
                if (domain != null) {
                    String domainPatterStr = "+(?i)^(http|https)://([a-z0-9]*\\.)*" + domain;
                    patterns.add(domainPatterStr);
                } else {
                    String protocolPatterStr = "+(?i)^(http|https)://*";
                    patterns.add(protocolPatterStr);
                    LOGGER.warn("Defaulting to basic url regex filtering (just suffix and protocol");
                }
            }
            urlFilter = new RegexUrlFilter(patterns.toArray(new String[patterns.size()]));

            // OK, now we're ready to start looping, since we've got our current
            // settings
            for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

                // Adjust target end time, if appropriate.
                if (hasEndTime) {
                    int remainingLoops = (endLoop - curLoop) + 1;
                    long now = System.currentTimeMillis();
                    long perLoopTime = (targetEndTime - now) / remainingLoops;
                    defaultPolicy.setCrawlEndTime(now + perLoopTime);
                }

                BasePath curLoopDirPath = CrawlDirUtils.makeLoopDir(platform, outputPath, curLoop);
                String curLoopDirName = curLoopDirPath.getName();
                setLoopLoggerFile(logsDir+curLoopDirName, curLoop);

                Flow flow = DemoCrawlWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, options); 
                flow.complete();
                
                // Writing out .dot files is a good way to verify your flows.
//              flow.writeDOT("build/valid-flow.dot");

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = platform.makePath(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }


}
