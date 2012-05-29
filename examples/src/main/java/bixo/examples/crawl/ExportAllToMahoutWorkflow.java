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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/30/12
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExportAllToMahoutWorkflow {
    private static final int EXPORT_STACKSIZE_KB = 128;

    @SuppressWarnings("serial")
    public static Flow createFlow( Path segmentsPath, ExportToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(EXPORT_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(ExportAllToMahoutWorkflow.class, false, conf);
        //create a merge of all sources in one flow
        int sourceNum = 0;

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(segmentsPath);
        Path[] segmentDirList = new Path[stats.length];
        int fileNum = 0;
        for( FileStatus s : stats ){
            segmentDirList[fileNum++] = s.getPath();
        }

        //get the mahout dir to write all mahout sequence files too
        Path sinkPath = new Path(options.getOutputDir());
        //todo we have a -ow so delete output path contents if it is set

        //create the Mahout output pipe
        Pipe mahoutExporterPipe = new Pipe("accumulate parsed text to mahout sequence file");
        mahoutExporterPipe = new Each(mahoutExporterPipe, new WriteMahoutSequenceFileFunction() );


        Fields outFields = new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN);
        Fields inFields = ParsedDatum.FIELDS;
        Path srcPath;
        Tap outputSink = new Hfs( new WritableSequenceFile(outFields, Text.class, Text.class), sinkPath.toString());
        ArrayList<Tap> all = new ArrayList<Tap>();
        for( Path pt : segmentDirList){
            srcPath = new Path( pt, "parse/part-00000");
            if( srcPath.getFileSystem(conf).exists(srcPath)){
                all.add(new Hfs( inFields, srcPath.toString()));
            }
        }
        Tap[] taps = all.toArray(new Tap[all.size()]);
        MultiSourceTap sources = new MultiSourceTap(taps);
        FlowConnector flowConnector = new FlowConnector( props );
        Flow flow = flowConnector.connect(sources, outputSink, mahoutExporterPipe);
        return flow;
    }
}
