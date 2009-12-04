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
import bixo.datum.UrlDatum;
import bixo.fetcher.FetchRequest;
import bixo.tools.sitecrawler.SiteCrawler;
import bixo.tools.sitecrawler.UrlImporter;
import bixo.urldb.IUrlFilter;
import bixo.utils.FsUtils;
import cascading.flow.PlannerException;

public class SimpleCrawlTool {
    private static final Logger LOGGER = Logger.getLogger(SimpleCrawlTool.class);

    private static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;

    private static final int MAX_CONTENT_SIZE = 128 * 1024;

    private static final long DEFAULT_CRAWL_DELAY = 5 * 1000L;

	private static final String WEB_ADDRESS = "http://bixo.101tec.com";

	private static final String EMAIL_ADDRESS = "bixo-dev@yahoogroups.com";
    
    // Limit the maximum number of requests made per connection to 50.
    @SuppressWarnings("serial")
    private static class MyFetchPolicy extends FetcherPolicy {
        public MyFetchPolicy() {
            super();
        }

        @Override
        public FetchRequest getFetchRequest(int maxUrls) {
            FetchRequest result = super.getFetchRequest(maxUrls);
            int numUrls = Math.min(50, result.getNumUrls());
            long nextTime = System.currentTimeMillis() + (numUrls * _crawlDelay);
            return new FetchRequest(numUrls, nextTime);
        }
    }

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

        String filename = String.format("%s/%d-SiteCrawlTool.log", outputDirName, loopNumber);
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

            // See if the user is starting from scratch
            if (!fs.exists(outputPath)) {
                fs.mkdirs(outputPath);

                // Create a "0-<timestamp>" sub-directory with just a /urls subdir
                // In the /urls dir the input file will have a single URL for the target domain.

                Path curLoopDir = FsUtils.makeLoopDir(fs, outputPath, 0);
                String curLoopDirName = curLoopDir.toUri().toString();
                setLoopLoggerFile(curLoopDirName, 0);

                UrlImporter importer = new UrlImporter(curLoopDir);
                importer.importOneDomain(domain, options.isDebugLogging());
            }
            
            Path inputPath = FsUtils.findLatestLoopDir(fs, outputPath);

            if (inputPath == null) {
                System.err.println("No previous cycle output dirs exist in " + outputDirName);
                printUsageAndExit(parser);
            }

            int startLoop = FsUtils.extractLoopNumber(inputPath);
            int endLoop = startLoop + options.getNumLoops();

            UserAgent userAgent = new UserAgent(options.getAgentName(), EMAIL_ADDRESS, WEB_ADDRESS);

            FetcherPolicy defaultPolicy = new MyFetchPolicy();
            defaultPolicy.setCrawlDelay(DEFAULT_CRAWL_DELAY);
            defaultPolicy.setMaxContentSize(MAX_CONTENT_SIZE);
            
            int crawlDurationInMinutes = options.getCrawlDuration();
            long targetEndTime = System.currentTimeMillis()
                            + (crawlDurationInMinutes * MILLISECONDS_PER_MINUTE);

            IUrlFilter urlFilter = new DomainUrlFilter(domain);

            // OK, now we're ready to start looping, since we've got our current
            // settings.
            for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

                // Adjust target end time, if appropriate.
                if (crawlDurationInMinutes != SimpleCrawlToolOptions.NO_CRAWL_DURATION) {
                    int remainingLoops = (endLoop - curLoop) + 1;
                    long now = System.currentTimeMillis();
                    long perLoopTime = (targetEndTime - now) / remainingLoops;
                    defaultPolicy.setCrawlEndTime(now + perLoopTime);
                }

                Path curLoopDir = FsUtils.makeLoopDir(fs, outputPath, curLoop);
                String curLoopDirName = curLoopDir.toUri().toString();
                setLoopLoggerFile(curLoopDirName, curLoop);

                SiteCrawler crawler = new SiteCrawler(inputPath, curLoopDir, userAgent,
                                defaultPolicy, options.getMaxThreads(), urlFilter);
                crawler.crawl(options.isDebugLogging());

                // Input for the next round is our current output
                inputPath = curLoopDir;
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
