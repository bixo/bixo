package bixo.examples.crawl;

import bixo.datum.ParsedDatum;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.WritableSequenceFile;
import cascading.tap.Hfs;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.bixolabs.cascading.HadoopUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/26/12
 * Time: 10:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExportToMahoutWorkflow {
    @SuppressWarnings("serial")
    public static Flow createFlow( ArrayList<Path> parsedTextPaths, NotSoSimpleCrawlToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(NotSoSimpleCrawlWorkflow.class, options.isDebugLogging(), conf);
        //create a merge of all sources in one flow
        int sourceNum = 0;

        //get the directory to output all mahout sequence files
        Path sinkPath = new Path( options.getMahoutDir());//don't get into this unless this is already been checked as set
        FileSystem fs = sinkPath.getFileSystem(conf);

        if(options.getOverwrite()){
            //delete contents of -mahoutdir
            fs.delete(sinkPath, true);//delete recursively now
        }

        //create the Mahout output pipe
        Pipe mahoutExporterPipe = new Pipe("accumulate parsed text to mahout sequence file");
        mahoutExporterPipe = new Each(mahoutExporterPipe, new WriteMahoutSequenceFileFunction() );


        Fields outFields = new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN);
        Fields inFields = ParsedDatum.FIELDS;
        Path srcPath;
        Tap outputSink = new Hfs( new WritableSequenceFile(outFields, Text.class, Text.class), sinkPath.toString());
        Tap[] taps = new Tap[parsedTextPaths.size()];
        int tapNum = 0;
        for( Path pt : parsedTextPaths){
            srcPath = new Path( pt, "parse/part-00000");
            taps[tapNum++] = new Hfs( inFields, srcPath.toString());
        }
        MultiSourceTap sources = new MultiSourceTap(taps);
        FlowConnector flowConnector = new FlowConnector( props );
        Flow flow = flowConnector.connect(sources, outputSink, mahoutExporterPipe);
        return flow;
    }
}
