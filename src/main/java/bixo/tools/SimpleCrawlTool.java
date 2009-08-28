package bixo.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.cascading.NullContext;
import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.LoggingFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.SimpleGroupingKeyGenerator;
import bixo.pipes.FetchPipe;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class SimpleCrawlTool {
    private static final Logger LOGGER = Logger.getLogger(SimpleCrawlTool.class);

    private static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;
    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;
    
    // max size of HTML that we will process (truncated if longer)
    private static final int MAX_CONTENT_SIZE = 128 * 1024;

    private static final String USER_AGENT_TEMPLATE = "Mozilla/5.0 (compatible; %s; +http://bixo.101tec.com; bixo-dev@yahoogroups.com";
    
    @SuppressWarnings("serial")
    private static class CreateUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            if (urlAsString.length() > 0) {
                try {
                    // Validaee the URL
                    new URL(urlAsString);
                    
                    UrlDatum urlDatum = new UrlDatum(urlAsString);
                    funcCall.getOutputCollector().add(urlDatum.toTuple());
                } catch (MalformedURLException e) {
                    LOGGER.error("Invalid URL in input data file: " + urlAsString);
                }
            }
        }
    }

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    private static JobConf getDefaultJobConf() throws IOException {
        JobClient jobClient = new JobClient(new JobConf());
        ClusterStatus status = jobClient.getClusterStatus();
        int trackers = status.getTaskTrackers();

        JobConf conf = new JobConf();
        conf.setNumMapTasks(trackers * 10);
        
        conf.setNumReduceTasks((trackers * conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2)));
        
        conf.setMapSpeculativeExecution( false );
        conf.setReduceSpeculativeExecution( false );
        conf.set("mapred.child.java.opts", "-server -Xmx512m -Xss128k");

        // Should match the value used for Xss above. Note no 'k' suffix for the ulimit command.
        // New support that one day will be in Hadoop.
        conf.set("mapred.child.ulimit.stack", "128");

        return conf;
    }

    private static Properties getDefaultProperties(CrawlToolOptions options, JobConf conf) throws IOException {
        Properties properties = new Properties();

        // Use special Cascading hack to control logging levels
        if( options.isDebugLogging() ) {
            properties.put("log4j.logger", "cascading=DEBUG,sharethis=DEBUG,bixo=TRACE");
        } else {
            properties.put("log4j.logger", "cascading=INFO,sharethis=INFO,bixo=INFO");
        }

        FlowConnector.setApplicationJarClass(properties, SimpleCrawlTool.class);

        // Propagate properties into the Hadoop JobConf
        MultiMapReducePlanner.setJobConf(properties, conf);

        return properties;
    }


    public static void main(String[] args) {
        CrawlToolOptions options = new CrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String inputFileName = options.getUrlInputFile();
        String outputDirName = options.getOutputDir();

        try {
            // Create the input (source tap), which is just a text file reader
            Tap in = new Hfs(new TextLine(), inputFileName);
            
            // Create the sink taps
            Tap status = new Hfs(new TextLine(StatusDatum.FIELDS.size()), outputDirName + "/status", true);
            Tap content = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD)), outputDirName + "/content", true);

            // Create the sub-assembly that runs the fetch job
            String userAgent = String.format(USER_AGENT_TEMPLATE, options.getAgentName());
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFunction());
            SimpleGroupingKeyGenerator grouping = new SimpleGroupingKeyGenerator(userAgent);
            LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
            
            // Set up appropriate default FetcherPolicy.
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setMaxContentSize(MAX_CONTENT_SIZE);
            int crawlDurationInMinutes = options.getCrawlDuration();
            if (crawlDurationInMinutes != CrawlToolOptions.NO_CRAWL_DURATION) {
                defaultPolicy.setCrawlEndTime(System.currentTimeMillis() + (crawlDurationInMinutes * MILLISECONDS_PER_MINUTE));
            }
            
            IHttpFetcher fetcher;
            if (options.isDryRun()) {
                fetcher = new LoggingFetcher(options.getMaxThreads());
            } else {
                fetcher = new SimpleHttpFetcher(options.getMaxThreads(), defaultPolicy, userAgent);
            }
            
            FetchPipe fetchPipe = new FetchPipe(importPipe, grouping, scoring, fetcher);

            LOGGER.info("Running fetch job with " + options);

            // Finally we can run it.
            JobConf conf = getDefaultJobConf();
            FlowConnector flowConnector = new FlowConnector(getDefaultProperties(options, conf));
            Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running SimpleCrawlTool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
