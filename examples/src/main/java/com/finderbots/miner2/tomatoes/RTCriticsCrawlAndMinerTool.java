/*
 * Copyright 2009-2012 Scale Unlimited
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
package com.finderbots.miner2.tomatoes;

import bixo.config.AdaptiveFetcherPolicy;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.flow.PlannerException;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import com.bixolabs.cascading.HadoopUtils;
import com.finderbots.miner2.*;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.List;

@SuppressWarnings("deprecation")
public class RTCriticsCrawlAndMinerTool {


    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    // Create log output file (in the local file system).
    private static void setLoopLoggerFile(String outputDirName, int loopNumber) {
        Logger rootLogger = Logger.getRootLogger();

        String filename = String.format("%s/%d-DemoCrawlAndMinerTool.log", outputDirName, loopNumber);
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

    public static void importOneDomain(String targetDomain, Path crawlDbPath, JobConf conf) throws Exception {
        
        try {
            Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
            TupleEntryCollector writer = urlSink.openForWrite(conf);
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

            CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0);

            writer.add(datum.getTuple());
            writer.close();
        } catch (Exception e) {
            HadoopUtils.safeRemove(crawlDbPath.getFileSystem(conf), crawlDbPath);
            throw e;
        }
    }

    private static void importUrls(String urlsFile, Path crawlDbPath) throws Exception {
        Path urlsPath = new Path(urlsFile);
        UrlImporter urlImporter = new UrlImporter(urlsPath, crawlDbPath);
        urlImporter.importUrls(false);
    }

    public static void main(String[] args) {
        Options options = new Options();
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
            JobConf conf = new JobConf();
            Path outputPath = new Path(outputDirName);
            FileSystem fs = outputPath.getFileSystem(conf);

            // First check if the user wants to clean
            if (options.isCleanOutputDir()) {
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
            }

            // See if the user isn't starting from scratch then set up the
            // output directory and create an initial urls subdir.
            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);

                // Create a "0-<timestamp>" sub-directory with just a /crawldb subdir
                // In the /crawldb dir the input file will have a single URL for the target domain.

                Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, 0);
                String curLoopDirName = curLoopDir.getName();
                setLoopLoggerFile(logsDir + curLoopDirName, 0);

                Path crawlDbPath = new Path(curLoopDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);

                if (domain != null) {
                    importOneDomain(domain, crawlDbPath, conf);
                } else {
                    importUrls(urlsFile, crawlDbPath);
                }
            }

            Path latestDirPath = CrawlDirUtils.findLatestLoopDir(fs, outputPath);

            if (latestDirPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                printUsageAndExit(parser);
            }

            Path crawlDbPath = new Path(latestDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

            // Set up the start and end loop counts.
            int startLoop = CrawlDirUtils.extractLoopNumber(latestDirPath);
            int endLoop = startLoop + options.getNumLoops();

            // Set up the UserAgent for the fetcher.
            UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);

            // You also get to customize the FetcherPolicy
            FetcherPolicy defaultPolicy;
            if(options.getCrawlDuration() != 0){
                defaultPolicy = new AdaptiveFetcherPolicy(options.getEndCrawlTime(), options.getCrawlDelay());
            } else {
                defaultPolicy = new FetcherPolicy();
            }
            defaultPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
            defaultPolicy.setRequestTimeout(10L*1000L);//10 seconds

            // COMPLETE for crawling a single site, EFFICIENT for many sites
            if(options.getCrawlPolicy().equals(Options.IMPOLITE_CRAWL_POLICY)){
                defaultPolicy.setFetcherMode(FetcherPolicy.FetcherMode.IMPOLITE);
            } else if(options.getCrawlPolicy().equals(Options.EFFICIENT_CRAWL_POLICY)){
                defaultPolicy.setFetcherMode(FetcherPolicy.FetcherMode.EFFICIENT);
            } else if (options.getCrawlPolicy().equals(Options.COMPLETE_CRAWL_POLICY)){
                defaultPolicy.setFetcherMode(FetcherPolicy.FetcherMode.COMPLETE);
            }

            // It is a good idea to set up a crawl duration when running long crawls as you may
            // end up in situations where the fetch slows down due to a 'long tail' and by
            // specifying a crawl duration you know exactly when the crawl will end.
            int crawlDurationInMinutes = options.getCrawlDuration();
            boolean hasEndTime = crawlDurationInMinutes != Options.NO_CRAWL_DURATION;
            long targetEndTime = hasEndTime ? System.currentTimeMillis() + (crawlDurationInMinutes * CrawlConfig.MILLISECONDS_PER_MINUTE) :
                FetcherPolicy.NO_CRAWL_END_TIME;

            // By setting up a url filter we only deal with urls that we want to
            // instead of all the urls that we extract.
            BaseUrlFilter urlFilter = null;
            List<String> patterns = null;
            String regexUrlFiltersFile = options.getRegexUrlFiltersFile();
            if (regexUrlFiltersFile != null) {
                patterns = RegexUrlDatumFilter.getUrlFilterPatterns(regexUrlFiltersFile);
            } else {
                patterns = RegexUrlDatumFilter.getDefaultUrlFilterPatterns();
                if (domain != null) {
                    String domainPatterStr = "+(?i)^(http|https)://([a-z0-9]*\\.)*" + domain;
                    patterns.add(domainPatterStr);
                } else {
                    String protocolPatterStr = "+(?i)^(http|https)://*";
                    patterns.add(protocolPatterStr);
                    //Log.warn("Defaulting to basic url regex filtering (just suffix and protocol");
                }
            }
            urlFilter = new RegexUrlDatumFilter(patterns.toArray(new String[patterns.size()]));

            // get a list of patterns which tell the miner which URLs to include or exclude.
            patterns.clear();
            RegexUrlStringFilter urlsToMineFilter = null;
            String regexUrlsToMineFiltersFile = options.getRegexUrlToMineFile();
            MineRTCriticsPreferences prefsAnalyzer = null;
            if (regexUrlsToMineFiltersFile != null ){
                patterns = RegexUrlDatumFilter.getUrlFilterPatterns(regexUrlsToMineFiltersFile);
                urlsToMineFilter = new RegexUrlStringFilter(patterns.toArray(new String[patterns.size()]));
                prefsAnalyzer = new MineRTCriticsPreferences(urlsToMineFilter);
            }

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

                Path curLoopDirPath = CrawlDirUtils.makeLoopDir(fs, outputPath, curLoop);
                String curLoopDirName = curLoopDirPath.getName();
                setLoopLoggerFile(logsDir+curLoopDirName, curLoop);

                Flow flow = RTCriticsCrawlAndMinerWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, prefsAnalyzer, options);
                flow.complete();
                
                // Writing out .dot files is a good way to verify your flows.
              flow.writeDOT("valid-flow.dot");

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = new Path(curLoopDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
        } catch (PlannerException e) {
            e.writeDOT("failed-flow.dot");
            System.err.println("PlannerException: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    public static class Options {

        public static final int NO_CRAWL_DURATION = 0;
        public static final Long DEFAULT_CRAWL_DELAY = 5L * 1000L;// 10 seconds between fetches?
        public static final int DEFAULT_MAX_THREADS = 10;
        private static final int DEFAULT_NUM_LOOPS = 1;
        private static final String DEFAULT_LOGS_DIR = "logs";
        private static Logger LOGGER = Logger.getRootLogger();
        public static final String EFFICIENT_CRAWL_POLICY = "EFFICIENT-CRAWL";
        public static final String COMPLETE_CRAWL_POLICY = "COMPLETE-CRAWL";
        public static final String IMPOLITE_CRAWL_POLICY = "IMPOLITE-CRAWL";
        public static final String DEFAULT_CRAWL_POLICY = EFFICIENT_CRAWL_POLICY;

        private String _loggingAppender = null;
        private boolean _debugLogging = false;
        private String _outputDir;
        private String _agentName;
        private String _domain;
        private String _urlsFile;
        private int _crawlDuration = NO_CRAWL_DURATION;
        private int _maxThreads = DEFAULT_MAX_THREADS;
        private int _numLoops = DEFAULT_NUM_LOOPS;
        private boolean _useBoilerpipe = false;
        private String _regexUrlFiltersFile = null;
        private String _logsDir = DEFAULT_LOGS_DIR;
        private Long _endCrawlTime = null;//no end time specified
        private Long _startCrawlTime = null;//no end time specified
        private boolean _cleanOutputDir = false;
        private boolean _generateHTML = false;
        private boolean _enableMiner = false;
        private String _regexUrlToMineFile = null;//default to mine all fetched pages
        private String _regexOutlinksToMineFile = null;//default ro return all outlinks
        private String _crawlPolicy = DEFAULT_CRAWL_POLICY;
        private Long _crawlDelay = DEFAULT_CRAWL_DELAY;

        Options(){
            _crawlPolicy = DEFAULT_CRAWL_POLICY;
        }

        @Option(name = "-delayBetweenFetches", usage = "Set the amount of time between fetches in Miliseconds (optional). Default: 10000 (10 seconds)", required = false)
        public void setCrawlDelay(Long crawlDelay) {
            this._crawlDelay = crawlDelay;//store as milliseconds
        }

        @Option(name = "-crawlPolicy", usage = "Policy for following links: EFFICIENT-CRAWL = follow all that are convenient, COMPLETE-CRAWL = follow all even if it means waiting, IMPOLITE-CRAWL = follow all and do it fast (optional). Default: EFFICIENT-CRAWL", required = false)
        public void setCrawlPolicy(String _crawlPolicy) {
            if(!_crawlPolicy.equals(EFFICIENT_CRAWL_POLICY) && !_crawlPolicy.equals(COMPLETE_CRAWL_POLICY) && !_crawlPolicy.equals(IMPOLITE_CRAWL_POLICY)){
                LOGGER.warn("Bad crawl policy, using default: EFFICIENT-CRAWL.");
                this._crawlPolicy = DEFAULT_CRAWL_POLICY;
            } else {
                this._crawlPolicy = _crawlPolicy;
            }
        }

        @Option(name = "-urlstomine", usage = "text file containing list of regex patterns for urls to mine", required = false)
        public void setRegexUrlToMineFile(String regexFiltersFile) {
            _regexUrlToMineFile = regexFiltersFile;
        }

        @Option(name = "-outlinkstomine", usage = "text file containing list of regex patterns for outlinks on the urltomine which will be returned as results", required = false)
        public void setRegexOutlinksToMineFile(String regexFiltersFile) {
            _regexOutlinksToMineFile = regexFiltersFile;
        }

        @Option(name = "-clean", usage = "Delete the output dir if it exists - WARNING:you won't be prompted!", required = false)
        public void setCleanOutputDir(boolean cleanOutputDir) {
            _cleanOutputDir = cleanOutputDir;
        }

        @Option(name = "-html", usage = "Generate HTML output as a text file", required = false)
        public void setGenerateHTML(boolean generateHTML) {
            _generateHTML = generateHTML;
        }

        @Option(name = "-enableminer", usage = "Generate miner output as a text file", required = false)
        public void setEnableMiner(boolean generateHTML) {
            _enableMiner = generateHTML;
        }

        @Option(name = "-domain", usage = "domain to crawl (e.g. cnn.com)", required = false)
        public void setDomain(String domain) {
            _domain = domain;
        }

        @Option(name = "-urls", usage = "text file containing list of urls (either -domain or -urls needs to be set)", required = false)
        public void setUrlsFile(String urlsFile) {
            _urlsFile = urlsFile;
        }

        @Option(name = "-d", usage = "debug logging", required = false)
        public void setDebugLogging(boolean debugLogging) {
            _debugLogging = debugLogging;
        }

        @Option(name = "-logger", usage = "set logging appender (console, DRFA)", required = false)
        public void setLoggingAppender(String loggingAppender) {
            _loggingAppender = loggingAppender;
        }

        @Option(name = "-outputdir", usage = "output directory", required = true)
        public void setOutputDir(String outputDir) {
            _outputDir = outputDir;
        }

        @Option(name = "-agentname", usage = "user agent name", required = true)
        public void setAgentName(String agentName) {
            _agentName = agentName;
        }

        @Option(name = "-maxthreads", usage = "maximum number of fetcher threads to use", required = false)
        public void setMaxThreads(int maxThreads) {
            _maxThreads = maxThreads;
        }

        @Option(name = "-numloops", usage = "number of fetch/update loops", required = false)
        public void setNumLoops(int numLoops) {
            _numLoops = numLoops;
        }

        @Option(name = "-duration", usage = "Target crawl duration in minutes (optional)", required = false)
        public void setCrawlDuration(int durationInMinutes) {
            _startCrawlTime = System.currentTimeMillis();
            _endCrawlTime = _startCrawlTime + (durationInMinutes * 60 * 1000);//store as millis
            _crawlDuration = durationInMinutes;
        }

        @Option(name = "-boilerpipe", usage = "Use Boilerpipe library when parsing", required = false)
        public void setUseBoilerpipe(boolean useBoilerpipe) {
            _useBoilerpipe = useBoilerpipe;
        }

        @Option(name = "-urlfilters", usage = "text file containing list of regex patterns for url filtering", required = false)
        public void setRegexUrlFiltersFile(String regexFiltersFile) {
            _regexUrlFiltersFile = regexFiltersFile;
        }

        @Option(name = "-logsdir", usage = "local fs dir to store loop specific logs [optional: default=logs]", required = false)
        public void setLogsDir(String logsDir) {
            _logsDir = logsDir;
        }

        public boolean isCleanOutputDir() {
            return _cleanOutputDir  ;
        }

        public boolean isGenerateHTML() {
            return _generateHTML  ;
        }

        public String getRegexOutlinksToMineFile() {
            return _regexOutlinksToMineFile ;

        }

        public String getCrawlPolicy() {
            return _crawlPolicy;
        }

        public String getRegexUrlToMineFile() {
            return _regexUrlToMineFile ;

        }

        public Long getCrawlDelay() {
            return _crawlDelay;
        }

        @Option(name = "-crawldelay", usage = "Crawl delay between fetches in seconds (optional). Default: 5", required = false)
        public Options setCrawlDelay( long crawlDelay) {
            _crawlDelay = crawlDelay;
            return this;
        }

        public Long getEndCrawlTime() {
            return _endCrawlTime;
        }

        public void setEndCrawlTime(Long _endCrawlTime) {
            this._endCrawlTime = _endCrawlTime;
        }

        public String getOutputDir() {
            return _outputDir;
        }

        public boolean isEnableMiner() {
            return _enableMiner  ;
        }

        public String getDomain() {
            return _domain;
        }

        public String getUrlsFile() {
            return _urlsFile;
        }

        public String getAgentName() {
            return _agentName;
        }

        public int getMaxThreads() {
            return _maxThreads;
        }

        public int getNumLoops() {
            return _numLoops;
        }

        public int getCrawlDuration() {
            return _crawlDuration;
        }

        public boolean isDebugLogging() {
            return _debugLogging;
        }

        public String getLoggingAppender() {
            return _loggingAppender;
        }

        public boolean isUseBoilerpipe() {
            return _useBoilerpipe ;
        }

        public String getRegexUrlFiltersFile() {
            return _regexUrlFiltersFile ;

        }

        public String getLogsDir() {
            return _logsDir ;

        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }
    }
}
