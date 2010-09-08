package bixo.tools;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import bixo.hadoop.HadoopUtils;
import bixo.urldb.IUrlFilter;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.flow.PlannerException;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;

public class SimpleCrawlTool {

    private static final Logger LOGGER = Logger.getLogger(JDBCCrawlTool.class);

    
    // Filter URLs that fall outside of the target domain
    @SuppressWarnings("serial")
    private static class DomainUrlFilter implements IUrlFilter {

        private String _domain;
        private Pattern _suffixExclusionPattern;
        private Pattern _protocolInclusionPattern;

        public DomainUrlFilter(String domain) {
            _domain = domain;
            _suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe)$");
            _protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
        }

        @Override
        public boolean isRemove(UrlDatum datum) {
            String urlAsString = datum.getUrl();
            
            // Skip URLs with protocols we don't want to try to process
            if (!_protocolInclusionPattern.matcher(urlAsString).find()) {
                return true;
            }

            if (_suffixExclusionPattern.matcher(urlAsString).find()) {
                return true;
            }
            
            try {
                URL url = new URL(urlAsString);
                String host = url.getHost();
                return (!host.endsWith(_domain));
            } catch (MalformedURLException e) {
                LOGGER.warn("Invalid URL: " + urlAsString);
                return true;
            }
        }
    }

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    // Create log output file in loop directory.
    private static void setLoopLoggerFile(String outputDirName, int loopNumber) {
        Logger rootLogger = Logger.getRootLogger();

        String filename = String.format("%s/%d-SimpleCrawlTool.log", outputDirName, loopNumber);
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
    
    private static void importOneDomain(String targetDomain, Path crawlDbPath, boolean debug) throws Exception {
        JobConf conf = HadoopUtils.getDefaultJobConf();
        
        try {
            Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
            TupleEntryCollector writer = urlSink.openForWrite(conf);

            CrawlDbDatum datum = new CrawlDbDatum("http://" + targetDomain, 0, 0, UrlStatus.UNFETCHED, 0);

            writer.add(datum.toTuple());
            writer.close();
        } catch (Exception e) {
            HadoopUtils.safeRemove(crawlDbPath.getFileSystem(conf), crawlDbPath);
            throw e;
        }
    }


    public static void main(String[] args) {
        SimpleCrawlToolOptions options = new SimpleCrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Before we get too far along, see if the domain looks valid.
        String domain = options.getDomain();
        if (domain.startsWith("http")) {
            System.err.println("The target domain should be specified as just the host, without the http protocol: " + domain);
            printUsageAndExit(parser);
        }
        
        if (!domain.equals("localhost") && (domain.split("\\.").length < 2)) {
            System.err.println("The target domain should be a valid paid-level domain or subdomain of the same: " + domain);
            printUsageAndExit(parser);
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
        
        try {
            JobConf conf = new JobConf();
            Path outputPath = new Path(outputDirName);
            FileSystem fs = outputPath.getFileSystem(conf);

            // See if the user isn't starting from scratch then set up the 
            // output directory and create an initial urls subdir.
            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);

                // Create a "0-<timestamp>" sub-directory with just a /urls subdir
                // In the /urls dir the input file will have a single URL for the target domain.

                Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, 0);
                String curLoopDirName = curLoopDir.toUri().toString();
                setLoopLoggerFile(curLoopDirName, 0);

                Path crawlDbPath = new Path(curLoopDir, SimpleCrawlConfig.CRAWLDB_SUBDIR_NAME);
                importOneDomain(domain, crawlDbPath, true);
            }
            
            Path latestDirPath = CrawlDirUtils.findLatestLoopDir(fs, outputPath);

            if (latestDirPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                printUsageAndExit(parser);
            }

            Path crawlDbPath = new Path(latestDirPath, SimpleCrawlConfig.CRAWLDB_SUBDIR_NAME);

            // Set up the start and end loop counts.
            int startLoop = CrawlDirUtils.extractLoopNumber(latestDirPath);
            int endLoop = startLoop + options.getNumLoops();

            // Set up the UserAgent for the fetcher.
            UserAgent userAgent = new UserAgent(options.getAgentName(), SimpleCrawlConfig.EMAIL_ADDRESS, SimpleCrawlConfig.WEB_ADDRESS);

            // You also get to customize the FetcherPolicy
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setCrawlDelay(SimpleCrawlConfig.DEFAULT_CRAWL_DELAY);
            defaultPolicy.setMaxContentSize(SimpleCrawlConfig.MAX_CONTENT_SIZE);
            defaultPolicy.setFetcherMode(FetcherMode.EFFICIENT);
            
            // It is a good idea to set up a crawl duration when running long crawls as you may 
            // end up in situations where the fetch slows down due to a 'long tail' and by 
            // specifying a crawl duration you know exactly when the crawl will end.
            int crawlDurationInMinutes = options.getCrawlDuration();
            boolean hasEndTime = crawlDurationInMinutes != JDBCCrawlToolOptions.NO_CRAWL_DURATION;
            long targetEndTime = hasEndTime ? System.currentTimeMillis()
                            + (crawlDurationInMinutes * SimpleCrawlConfig.MILLISECONDS_PER_MINUTE) : FetcherPolicy.NO_CRAWL_END_TIME;

            // By setting up a url filter we only deal with urls that we want to 
            // instead of all the urls that we extract.
            IUrlFilter urlFilter = new DomainUrlFilter(domain);

            // OK, now we're ready to start looping, since we've got our current settings
            for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

                // Adjust target end time, if appropriate.
                if (hasEndTime) {
                    int remainingLoops = (endLoop - curLoop) + 1;
                    long now = System.currentTimeMillis();
                    long perLoopTime = (targetEndTime - now) / remainingLoops;
                    defaultPolicy.setCrawlEndTime(now + perLoopTime);
                }

                Path curLoopDirPath = CrawlDirUtils.makeLoopDir(fs, outputPath, curLoop);
                String curLoopDirName = curLoopDirPath.toUri().toString();
                setLoopLoggerFile(curLoopDirName, curLoop);

                Flow flow = SimpleCrawlWorkflow.createFlow(curLoopDirPath, crawlDbPath, defaultPolicy, userAgent, urlFilter, options); 
                flow.complete();
                
                // Writing out .dot files is a good way to verify your flows.
//              flow.writeDOT("build/valid-flow.dot");

                // Update crawlDbPath to point to the latest crawl db
                crawlDbPath = new Path(curLoopDirPath, SimpleCrawlConfig.CRAWLDB_SUBDIR_NAME);
            }
        } catch (PlannerException e) {
            e.writeDOT("build/failed-flow.dot");
            System.err.println("PlannerException: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }


}
