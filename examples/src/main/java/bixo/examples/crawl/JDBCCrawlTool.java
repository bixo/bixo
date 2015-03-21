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

//import java.io.IOException;
//import java.util.List;
//
//import org.apache.log4j.FileAppender;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PatternLayout;
//import org.kohsuke.args4j.CmdLineException;
//import org.kohsuke.args4j.CmdLineParser;
//
//import bixo.config.BixoPlatform;
//import bixo.config.FetcherPolicy;
//import bixo.config.FetcherPolicy.FetcherMode;
//import bixo.config.UserAgent;
//import bixo.datum.UrlStatus;
//import bixo.urls.BaseUrlFilter;
//import bixo.urls.SimpleUrlNormalizer;
//import bixo.utils.CrawlDirUtils;
//import cascading.flow.Flow;
//import cascading.tap.Tap;
//import cascading.tuple.TupleEntryCollector;
//
//import com.scaleunlimited.cascading.BasePath;
//import com.scaleunlimited.cascading.BasePlatform;

/**
 * JDBCCrawlTool is an example of using Bixo to write a simple crawl tool.
 * 
 * This tool uses an in-memory hsqldb to demonstrate how one could use a 
 * database to maintain the crawl db. 
 *  
 * NOTE:This tool relies on cascading-jdbc which hasn't been updated for Cascading 2.x.
 * Until we have a version of cascading-jdbc that works with Cascading 2.x, the tool will be commented
 */
public class JDBCCrawlTool {
    /*
    private static final Logger LOGGER = Logger.getLogger(JDBCCrawlTool.class);

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    // Create log output file (in the local file system).
    private static void setLoopLoggerFile(String outputDirName, int loopNumber) {
        Logger rootLogger = Logger.getRootLogger();

        String filename = String.format("%s/%d-JDBCCrawlTool.log", outputDirName, loopNumber);
        FileAppender appender = (FileAppender) rootLogger.getAppender("loop-logger");
        if (appender == null) {
            appender = new FileAppender();
            appender.setName("loop-logger");
            appender.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}:%L - %m%n"));

            // We have to do this before calling addAppender, as otherwise Log4J
            // warns us.
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


    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void importOneDomain(String targetDomain, Tap urlSink, BasePlatform platform) throws Exception {

        TupleEntryCollector writer;
        try {
            writer = urlSink.openForWrite(platform.makeFlowProcess());
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
            CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0);

            writer.add(datum.getTuple());
            writer.close();
        } catch (IOException e) {
            throw e;
        }
    }


    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        JDBCCrawlToolOptions options = new JDBCCrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Before we get too far along, see if the domain looks valid.
        String domain = options.getDomain();
        if (domain != null) {
            validateDomain(domain, parser);
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
            BixoPlatform platform = new BixoPlatform(options.isLocalMode());
            BasePath outputPath = platform.makePath(outputDirName);

            // See if the user is starting from scratch
            if (options.getDbLocation() == null) {
                if (outputPath.exists()) {
                    System.out.println("Warning: Previous cycle output dirs exist in : " + outputDirName);
                    System.out.println("Warning: Deleting the output dir before running");
                    outputPath.delete(true);
                }
            } else {
                BasePath dbLocationPath = platform.makePath(options.getDbLocation());
                if (!dbLocationPath.exists()) {
                    dbLocationPath.mkdirs();
                }
            }

            if (!outputPath.exists()) {
                outputPath.mkdirs();

                BasePath curLoopDir = CrawlDirUtils.makeLoopDir(platform, outputPath, 0);
                String curLoopDirName = curLoopDir.getName();
                setLoopLoggerFile(logsDir + curLoopDirName, 0);

                if (domain == null) {
                    System.err.println("For a new crawl the domain needs to be specified" + domain);
                    printUsageAndExit(parser);
                }
                importOneDomain(domain, JDBCTapFactory.createUrlsSinkJDBCTap(platform, options.getDbLocation()), platform);
            }

            BasePath inputPath = CrawlDirUtils.findLatestLoopDir(platform, outputPath);

            if (inputPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                printUsageAndExit(parser);
            }

            int startLoop = CrawlDirUtils.extractLoopNumber(inputPath);
            int endLoop = startLoop + options.getNumLoops();

            UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);

            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
            defaultPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
            defaultPolicy.setFetcherMode(FetcherMode.EFFICIENT);

            int crawlDurationInMinutes = options.getCrawlDuration();
            boolean hasEndTime = crawlDurationInMinutes != JDBCCrawlToolOptions.NO_CRAWL_DURATION;
            long targetEndTime = hasEndTime ? 
                            System.currentTimeMillis() + (crawlDurationInMinutes * CrawlConfig.MILLISECONDS_PER_MINUTE) : FetcherPolicy.NO_CRAWL_END_TIME;

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

            // Now we're ready to start looping, since we've got our current settings
            for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

                // Adjust target end time, if appropriate.
                if (hasEndTime) {
                    int remainingLoops = (endLoop - curLoop) + 1;
                    long now = System.currentTimeMillis();
                    long perLoopTime = (targetEndTime - now) / remainingLoops;
                    defaultPolicy.setCrawlEndTime(now + perLoopTime);
                }

                BasePath curLoopDir = CrawlDirUtils.makeLoopDir(platform, outputPath, curLoop);
                String curLoopDirName = curLoopDir.getName();
                setLoopLoggerFile(logsDir+curLoopDirName, curLoop);

                Flow flow = JDBCCrawlWorkflow.createFlow(platform, inputPath, curLoopDir, userAgent, defaultPolicy, urlFilter, 
                                options.getMaxThreads(), options.isDebugLogging(), options.getDbLocation());
                flow.complete();
                // flow.writeDOT("build/valid-flow.dot");

                // Input for the next round is our current output
                inputPath = curLoopDir;
            }
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        JDBCTapFactory.shutdown();
    }
*/
}
