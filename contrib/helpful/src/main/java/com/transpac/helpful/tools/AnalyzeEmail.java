package com.transpac.helpful.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.pipes.FetchPipe;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.MultiMapReducePlanner;
import cascading.flow.PlannerException;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.bixolabs.cascading.NullContext;
import com.transpac.helpful.operations.CalcMessageScoreBuffer;
import com.transpac.helpful.operations.FieldNames;
import com.transpac.helpful.operations.MboxSplitterFunction;
import com.transpac.helpful.operations.ParseEmailFunction;
import com.transpac.helpful.operations.ParseModMboxPageFunction;
import com.transpac.helpful.operations.SumScoresBuffer;

public class AnalyzeEmail {
	private static final Logger LOGGER = Logger.getLogger(AnalyzeEmail.class);
	
	private static final String WEB_ADDRESS = "http://wiki.github.com/bixo/bixo/bixocrawler";

	private static final String EMAIL_ADDRESS = "bixo-dev@yahoogroups.com";
    
	private static final int MAX_CONTENT_SIZE = 8 * 1024 * 1024;

	private static final int MAX_THREADS = 1;
	private static final int NUM_REDUCERS = 1;
	
	private static final String MBOX_PAGE_STATUS_PIPE_NAME = "mbox page fetch status pipe";
	private static final String SPLITTER_PIPE_NAME = "Split emails pipe";
	private static final String ANALYZER_PIPE_NAME = "Analyze emails pipe";

	@SuppressWarnings("serial")
	private static class LoadUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {
		public LoadUrlFunction() {
            super(UrlDatum.FIELDS);
		}

		@Override
		public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String url = funcCall.getArguments().getString("line");
            if ((url.length() == 0) || (url.startsWith("#"))) {
            	return;
            }
            
        	try {
            	// Validate the URL
                new URL(url);
                UrlDatum urlDatum = new UrlDatum(url);
                funcCall.getOutputCollector().add(urlDatum.getTuple());
            } catch (MalformedURLException e) {
                LOGGER.error("Invalid URL in input data file: " + url);
            }
		}
	}

    @SuppressWarnings("serial")
	private static class SplitEmails extends SubAssembly {

		public SplitEmails(FetchPipe fetchPipe) {
            Pipe splitPipe = new Pipe(SPLITTER_PIPE_NAME, fetchPipe.getContentTailPipe());
            splitPipe = new Each(splitPipe, new MboxSplitterFunction());
            // TODO KKr - code currently relies on splitPipe being first tail pipe.
            setTails(splitPipe, fetchPipe.getStatusTailPipe());
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

    private static Properties getDefaultProperties(AnalyzeEmailOptions options, JobConf conf) throws IOException {
        Properties properties = new Properties();

        // Use special Cascading hack to control logging levels
        if( options.isDebugLogging() ) {
            properties.put("log4j.logger", "cascading=DEBUG,sharethis=DEBUG,bixo=TRACE");
        } else {
            properties.put("log4j.logger", "cascading=INFO,sharethis=INFO,bixo=INFO");
        }

        FlowConnector.setApplicationJarClass(properties, AnalyzeEmail.class);

        // Propagate properties into the Hadoop JobConf
        MultiMapReducePlanner.setJobConf(properties, conf);

        return properties;
    }



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AnalyzeEmailOptions options = new AnalyzeEmailOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        String inputFileName = options.getInputFile();
        String outputDirName = options.getOutputDir();

        try {
            // Create the input (source tap), which is just a text file reader
            Tap sourceTap = new Hfs(new TextLine(), inputFileName);
            
            // Create the sub-assembly that runs the fetch job
            UserAgent userAgent = new UserAgent(options.getAgentName(), EMAIL_ADDRESS, WEB_ADDRESS);
            Pipe importPipe = new Each("url importer", new Fields("line"), new LoadUrlFunction());
            
            BaseScoreGenerator scorer = new FixedScoreGenerator();
            
            BaseFetcher fetcher = new SimpleHttpFetcher(MAX_THREADS, userAgent);
            FetchPipe fetchPagePipe = new FetchPipe(importPipe, scorer, fetcher, NUM_REDUCERS);
            
            // Here's the pipe that will output UrlDatum tuples, by extracting URLs from the mod_mbox-generated page.
    		Pipe mboxPagePipe = new Each(fetchPagePipe.getContentTailPipe(), new ParseModMboxPageFunction(), Fields.RESULTS);

    		// Create a named pipe for the status of the mod_mbox-generated pages.
            Pipe mboxPageStatusPipe = new Pipe(MBOX_PAGE_STATUS_PIPE_NAME, fetchPagePipe.getStatusTailPipe());

            // Set up appropriate FetcherPolicy, where we increase the max content size (since mailbox files
            // can be big, e.g. 4MB).
            FetcherPolicy defaultPolicy = new FetcherPolicy();
            defaultPolicy.setMaxContentSize(MAX_CONTENT_SIZE);
            fetcher = new SimpleHttpFetcher(MAX_THREADS, defaultPolicy, userAgent);
            
            // We can create the fetch pipe, and set up our Mbox splitter to run on content.
            FetchPipe fetchMboxPipe = new FetchPipe(mboxPagePipe, scorer, fetcher, NUM_REDUCERS);
            SplitEmails splitterPipe = new SplitEmails(fetchMboxPipe);
            
            // Now create the pipe that's going to analyze the emails we get after splitting them up.
            Pipe analysisPipe = new Pipe(ANALYZER_PIPE_NAME, splitterPipe.getTails()[0]);
            analysisPipe = new Each(analysisPipe, new ParseEmailFunction());
            
            // We'll get output that has ANALYZED_EMAIL_FIELDS in it. We want to group by
            // the message-id field, and then do an aggregation on that of the scores.
            analysisPipe = new GroupBy(analysisPipe, new Fields(FieldNames.MESSAGE_ID));
            analysisPipe = new Every(analysisPipe, new CalcMessageScoreBuffer(), Fields.RESULTS);

            // Now we want to sum the scores for each user, which is another grouping/summing.
            analysisPipe = new GroupBy(analysisPipe, new Fields(FieldNames.EMAIL_ADDRESS));
            analysisPipe = new Every(analysisPipe, new SumScoresBuffer(), Fields.RESULTS);
            
            // Let's filter out anybody with an uninteresting score.
            ExpressionFilter filter = new ExpressionFilter(String.format("%s <= 0.0", FieldNames.SUMMED_SCORE), Double.class);
            analysisPipe = new Each(analysisPipe, filter);
            
            // And let's sort in reverse order (high to low score)
            analysisPipe = new GroupBy(analysisPipe, new Fields(FieldNames.SUMMED_SCORE), true);

            // Create the sink taps
            Tap pageStatusSinkTap = new Hfs(new TextLine(StatusDatum.FIELDS.size()), outputDirName + "/page-status", true);
            Tap mboxStatusSinkTap = new Hfs(new TextLine(StatusDatum.FIELDS.size()), outputDirName + "/mbox-status", true);
            Tap contentSinkTap = new Hfs(new SequenceFile(FetchedDatum.FIELDS), outputDirName + "/content", true);
            Tap analyzerSinkTap = new Hfs(new TextLine(), outputDirName + "/analysis", true);

            HashMap<String, Tap> sinkTapMap = new HashMap<String, Tap>(2);
            sinkTapMap.put(MBOX_PAGE_STATUS_PIPE_NAME, pageStatusSinkTap);
            sinkTapMap.put(FetchPipe.STATUS_PIPE_NAME, mboxStatusSinkTap);
            sinkTapMap.put(SPLITTER_PIPE_NAME, contentSinkTap);
            sinkTapMap.put(ANALYZER_PIPE_NAME, analyzerSinkTap);
            
            LOGGER.info("Running fetch job with " + options);

            // Finally we can run it.
            JobConf conf = getDefaultJobConf();
            FlowConnector flowConnector = new FlowConnector(getDefaultProperties(options, conf));
            Flow flow = flowConnector.connect(sourceTap, sinkTapMap, splitterPipe, mboxPageStatusPipe, analysisPipe);
            flow.writeDOT("build/goodFlow.dot");
            flow.complete();
        } catch (PlannerException e) {
            System.err.println("PlannerException running AnalyzeEmail: " + e.getMessage());
            e.writeDOT("build/failedFlow.dot");
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running AnalyzeEmail: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

	}

}
