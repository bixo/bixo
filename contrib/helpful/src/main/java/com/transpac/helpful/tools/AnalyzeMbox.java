package com.transpac.helpful.tools;


import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.transpac.helpful.operations.CalcMessageScoreBuffer;
import com.transpac.helpful.operations.FieldNames;
import com.transpac.helpful.operations.ParseEmailFunction;
import com.transpac.helpful.operations.SumScoresBuffer;

import bixo.datum.FetchedDatum;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;
import cascading.flow.PlannerException;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class AnalyzeMbox {
	
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

    private static Properties getDefaultProperties(AnalyzeMboxOptions options, JobConf conf) throws IOException {
        Properties properties = new Properties();

        // Use special Cascading hack to control logging levels
        if( options.isDebugLogging() ) {
            properties.put("log4j.logger", "cascading=DEBUG,bixo=DEBUG,helpful=TRACE");
        } else {
            properties.put("log4j.logger", "cascading=INFO,bixo=INFO,helpful=TRACE");
        }

        FlowConnector.setApplicationJarClass(properties, AnalyzeEmail.class);

        // Propagate properties into the Hadoop JobConf
        MultiMapReducePlanner.setJobConf(properties, conf);

        return properties;
    }


	public static void main(String[] args) {
		AnalyzeMboxOptions options = new AnalyzeMboxOptions();
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
            // Create the input (source tap), which is just a sequence file reader. We assume
        	// that the file already has the results of splitting the mbox file into emails.
            Tap sourceTap = new Hfs(new SequenceFile(FetchedDatum.FIELDS), inputFileName);
            
            Pipe pipe = new Pipe("Email Analyzer");
            pipe = new Each(pipe, new ParseEmailFunction());
            
            // We'll get output that has ANALYZED_EMAIL_FIELDS in it. We want to group by
            // the message-id field, and then do an aggregation on that of the scores.
            pipe = new GroupBy(pipe, new Fields(FieldNames.MESSAGE_ID));
            pipe = new Every(pipe, new CalcMessageScoreBuffer(), Fields.RESULTS);

            // Now we want to sum the scores for each user, which is another grouping/summing.
            pipe = new GroupBy(pipe, new Fields(FieldNames.EMAIL_ADDRESS));
            pipe = new Every(pipe, new SumScoresBuffer(), Fields.RESULTS);
            
            // Let's filter out anybody with an uninteresting score.
            ExpressionFilter filter = new ExpressionFilter(String.format("%s <= 0.0", FieldNames.SUMMED_SCORE), Double.class);
            pipe = new Each(pipe, filter);
            
            // And let's sort in reverse order (high to low score)
            pipe = new GroupBy(pipe, new Fields(FieldNames.SUMMED_SCORE), true);

            // Create the output (sink tap)
            Tap sinkTap = new Hfs(new TextLine(), outputDirName, true);
            
            // Finally we can run it.
            JobConf conf = getDefaultJobConf();
            FlowConnector flowConnector = new FlowConnector(getDefaultProperties(options, conf));
            Flow flow = flowConnector.connect(sourceTap, sinkTap, pipe);
            flow.complete();
        } catch (PlannerException e) {
            System.err.println("PlannerException running AnalyzeMbox: " + e.getMessage());
            e.writeDOT("build/failedFlow.dot");
            System.exit(-1);
        } catch (Throwable t) {
            System.err.println("Exception running AnalyzeMbox: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

	}

}
