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
    public static Flow createFlow( Path loopsPath, ExportToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(EXPORT_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(ExportAllToMahoutWorkflow.class, false, conf);
        //create a merge of all sources in one flow
        int sourceNum = 0;

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(loopsPath);
        Path[] segmentDirList = new Path[stats.length];
        Path currentLoopDir;
        int fileNum = 0;
        ArrayList<Tap> all = new ArrayList<Tap>();
        Fields inFields = ParsedDatum.FIELDS;
        for( FileStatus s : stats ){
            currentLoopDir = s.getPath();
            Path parseDir = new Path(currentLoopDir, "parse");
            FileStatus[] parsedFilesStats = fs.listStatus(parseDir);
            System.out.println("Path of parse: "+parseDir.toString()+ '\n');
            if(parsedFilesStats != null ){//got a 'parse' dir with files
                for(FileStatus f : parsedFilesStats){
                    if(f.getPath().toString().contains("part-")){//found a part-xxxxx file
                        Path filePath = new Path(f.getPath().toString());
                        all.add(new Hfs( inFields, filePath.toString()));
                    }
                }
            }
        }
        Tap[] taps = all.toArray(new Tap[all.size()]);

        //get the mahout dir to write all mahout sequence files too
        Path sinkPath = new Path(options.getOutputDir());
        //delete any previous files if -ow option
        if(options.getOverWrite() && !sinkPath.toString().equals("")){
            fs.delete(sinkPath, true);//delete with all sub dirs
        }

        //create the Mahout output pipe
        Pipe mahoutExporterPipe = new Pipe("accumulate parsed text to mahout sequence file");
        mahoutExporterPipe = new Each(mahoutExporterPipe, new WriteMahoutSequenceFileFunction() );


        Fields outFields = new Fields(ParsedDatum.URL_FN, ParsedDatum.PARSED_TEXT_FN);
        Path srcPath;
        Tap outputSink = new Hfs( new WritableSequenceFile(outFields, Text.class, Text.class), sinkPath.toString());
        MultiSourceTap sources = new MultiSourceTap(taps);
        FlowConnector flowConnector = new FlowConnector( props );
        Flow flow = flowConnector.connect(sources, outputSink, mahoutExporterPipe);
        return flow;
    }
}
