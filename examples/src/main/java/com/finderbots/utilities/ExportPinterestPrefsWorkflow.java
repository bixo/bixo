package com.finderbots.utilities;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.NullContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: pferrel
 * Date: 4/30/12
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExportPinterestPrefsWorkflow {
    private static final int EXPORT_STACKSIZE_KB = 128;
    private static final String PERSON_INDEX_SUBDIR = "person-index";
    private static final String PREFERENCES_SUBDIR = "preferences";
    private static int highestID;
    private static HashMap<String, Integer> index;

    public static int getHighestID() {
        return highestID;
    }

    public static void setHighestID(int highestID) {
        ExportPinterestPrefsWorkflow.highestID = highestID;
    }

    public static HashMap<String, Integer> getIndex() {
        return index;
    }

    public static void setIndex(HashMap<String, Integer> index) {
        ExportPinterestPrefsWorkflow.index = index;
    }

    private static class MergeFieldsFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public MergeFieldsFunction( Fields fields ) {
            super(fields);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            super.prepare(process, operationCall);
        }

        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            //To change body of implemented methods use File | Settings | File Templates.
            HashMap<String, Integer> index = ExportPinterestPrefsWorkflow.getIndex();
            Tuple tuple = funcCall.getArguments().getTuple();
            TupleEntryCollector collector = funcCall.getOutputCollector();
            Integer hashedPersonId1 = index.get(tuple.getString(0));
            Integer hashedPersonId2 = index.get(tuple.getString(1));
            Tuple tuple1 = new Tuple();
            tuple1.add(hashedPersonId1);
            //tuple1.add(index.get(tuple.getString(0)));//assumes it is already in the index!!!
            tuple1.add(tuple.getString(0));

            Tuple tuple2 = new Tuple();
            tuple2.add(hashedPersonId2);
            //tuple2.add(index.get(tuple.getString(1)));//assumes it is already in the index!!!
            tuple2.add(tuple.getString(1));

            IndexDatum id1 = new IndexDatum( tuple1 );
            IndexDatum id2 = new IndexDatum( tuple2 );
            collector.add(id1.getTuple());
            collector.add(id2.getTuple());
        }
    }

    private static class WriteHashedPrefPairs extends BaseOperation<NullContext> implements Function<NullContext> {

        public WriteHashedPrefPairs( Fields fields ) {
            super(fields);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            super.prepare(process, operationCall);
        }

        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            //To change body of implemented methods use File | Settings | File Templates.
            Tuple tuple = funcCall.getArguments().getTuple();
            TupleEntryCollector collector = funcCall.getOutputCollector();
            Tuple outTuple = new Tuple();
            String inKey = tuple.getString(0);
            String inValue = tuple.getString(1);
            int outKey;
            int outValue;
            HashMap<String, Integer> index = ExportPinterestPrefsWorkflow.getIndex();
            int highestID = ExportPinterestPrefsWorkflow.getHighestID();
            if( index.containsKey(inKey) ){
                outKey = index.get(inKey);
            } else { //new id-string mapping
                outKey = highestID;
                index.put(inKey, outKey);
                highestID += 1;
            }
            if( index.containsKey(inValue) ){
                outValue = index.get(inValue);
            } else { //new id-string mapping
                outValue = highestID;
                index.put(inValue, outValue);
                highestID += 1;
            }
            outTuple.add(outKey);
            outTuple.add(outValue+"\t1.0");
            collector.add(outTuple);
            ExportPinterestPrefsWorkflow.setHighestID(highestID);
            ExportPinterestPrefsWorkflow.setIndex(index);//todo not sure this is needed if index is a reference to enclosing class's index it's already modified. If it's a deep copy yikes, copied with every get?????
        }
    }
    
    public static Flow createFlow( Path crawlPath, ExportToolOptions options) throws Throwable {
        JobConf conf = HadoopUtils.getDefaultJobConf(EXPORT_STACKSIZE_KB);
        int numReducers = HadoopUtils.getNumReducers(conf);
        conf.setNumReduceTasks(numReducers);
        Properties props = HadoopUtils.getDefaultProperties(ExportPinterestPrefsWorkflow.class, false, conf);
        //create a merge of all sources to go into a multisource tap
        //Scheme importScheme = new TextLine(new Fields("line","text_person_id", "text_preference_id"));//two fields per line
        Scheme indexScheme = new TextLine( IndexDatum.FIELDS, 1 );
        indexScheme.setNumSinkParts(1);//one index file
        Scheme outputScheme = new TextLine( new Fields("hashed person id", "hashed preference id"));
        outputScheme.setNumSinkParts(1);//one preference output file

        //initialize the indexing stuff
        highestID = 0;//start sequential IDs from 0
        index = new HashMap<String, Integer>();

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(crawlPath);
        Path currentLoopDir;
        ArrayList<Tap> all = new ArrayList<Tap>();
        for( FileStatus s : stats ){
            currentLoopDir = s.getPath();
            Path minerDir = new Path(currentLoopDir, "miner");
            FileStatus[] parsedFilesStats = fs.listStatus(minerDir);
            System.out.println("Path of miner data: "+minerDir.toString()+ '\n');
            if(parsedFilesStats != null ){//got a 'parse' dir with files
                for(FileStatus f : parsedFilesStats){
                    if(f.getPath().toString().contains("part-")){//found a part-xxxxx file
                        Path filePath = new Path(f.getPath().toString());
                        //Tap t = new Hfs( inFields, filePath.toString());
                        //Tap t = new Hfs(new TextLine(), filePath.toString(), true);
                        Tap t = new Hfs(new TextLine(),filePath.toString(), true);
                        TapIterator ti = new TapIterator(t, conf);
                        if( ti.hasNext() ){// then part file is not empty
                            all.add(t);
                        }
                    }
                }
            }
        }
        Tap[] sourceTaps = all.toArray(new Tap[all.size()]);
        MultiSourceTap source = new MultiSourceTap(sourceTaps);

        //delete with all sub dirs by default, not set up to have more than one export
        //since the index needs to be all inclusive and is created here
        fs.delete(new Path(options.getOutputDir()), true);

        //get the dir to write all preference files to
        Path prefSinkPath = new Path(options.getOutputDir(), PREFERENCES_SUBDIR);

        //get the subdir to write the person index to
        Path personIndexSinkPath = new Path(options.getOutputDir(), PERSON_INDEX_SUBDIR);
        //todo: may have trouble if we need to create non-existent subdirs after the delete we'll see

        // split the text line into "url" and "raw" with the delimiter of comma
        RegexSplitter regexSplitter = new RegexSplitter( new Fields( "text_person_id", "text_preference_id" ), "," );
        Pipe importPipe = new Each( "import_preferences", new Fields( "line" ), regexSplitter, new Fields("text_person_id", "text_preference_id") );

        //merge the two id fields into one stream of ids since they are both really person ids
        //this is to prepare for indexing the unique ones.
        //take a Pipe instance, an argument selector, Operation instance, and a output selector on the constructor
        Pipe mergeFieldsPipe = new Pipe("merge fields", importPipe);
        MergeFieldsFunction mf = new MergeFieldsFunction( IndexDatum.FIELDS);
        mergeFieldsPipe = new Each(mergeFieldsPipe, new Fields( "text_person_id", "text_preference_id" ), mf, IndexDatum.FIELDS);

        //Pipe indexPipe = new Pipe("write out the index", importPipe,new Fields("text_person_id"));
        //Pipe indexPipe = new Unique(textIdPipe,);
        //indexPipe = new Each("hash_and_save",new HashAndSave());

        //now process input into an index sink
        Pipe unique = new Unique("force person id to be unique", mergeFieldsPipe, IndexDatum.FIELDS);

        //create a pipe tp write out hashed id pairs for preferences, which makes mahout happier
        Pipe writeHashedIds = new Pipe("write hashed preference pairs", importPipe);
        writeHashedIds = new Each(writeHashedIds, new Fields("text_person_id", "text_preference_id"), new WriteHashedPrefPairs( new Fields("hashed person id", "hashed preference id")), new Fields("hashed person id", "hashed preference id") );

        //now throw in a gratuitous GroupBy just to get a reducer so the output will go to one file (see the outputSink Scheme)
        GroupBy outputPipe = new GroupBy(writeHashedIds,new Fields("hashed person id", "hashed preference id"));

        Tap indexSink = new Hfs( indexScheme, personIndexSinkPath.toString());
        Tap outputSink = new Hfs( outputScheme, prefSinkPath.toString());
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        sinkMap.put(unique.getName(), indexSink);
        sinkMap.put(writeHashedIds.getName(), outputSink);

        List<Pipe> tailPipes = new ArrayList<Pipe>();
        tailPipes.add(unique);
        tailPipes.add(outputPipe);


        FlowConnector flowConnector = new FlowConnector( props );
        //Flow flow = flowConnector.connect(source, outputSink, unique);


        Flow flow = flowConnector.connect(source, sinkMap, tailPipes.toArray(new Pipe[tailPipes.size()]));
        return flow;
    }
}
