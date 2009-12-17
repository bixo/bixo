package bixo.tools.sitecrawler;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.cascading.NullContext;
import bixo.datum.UrlDatum;
import bixo.hadoop.HadoopUtils;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

public class UrlImporter {
	
    @SuppressWarnings("serial")
    private static class CreateUrlFromTextFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    	private static final Logger LOGGER = Logger.getLogger(CreateUrlFromTextFunction.class);
		
        public CreateUrlFromTextFunction() {
            super(UrlDatum.FIELDS.append(MetaData.FIELDS));
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            if (urlAsString.length() > 0) {
                try {
                    // Validate the URL
                    new URL(urlAsString);
                    
                    UrlDatum urlDatum = new UrlDatum(urlAsString);
                    urlDatum.addMetaDataValue("crawl-depth", "0");

                    funcCall.getOutputCollector().add(urlDatum.toTuple());
                } catch (MalformedURLException e) {
                    LOGGER.error("Invalid URL in input data file: " + urlAsString);
                }
            }
        }
    }

	private String _inputFilename;
	private Path _outputDir;
	
    public UrlImporter(String inputFilename, Path outputDir) {
        _inputFilename = inputFilename;
        _outputDir = outputDir;
    }
    
    public UrlImporter(Path outputDir) {
        _inputFilename = null;
        _outputDir = outputDir;
    }
    
	public void importOneDomain(String targetDomain, boolean debug) throws Exception {
        JobConf conf = HadoopUtils.getDefaultJobConf();
        FileSystem fs = _outputDir.getFileSystem(conf);
        
        try {
            String loopDirName = _outputDir.toUri().toString();
            Tap urlSink = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), loopDirName + "/urls", true);
            TupleEntryCollector writer = urlSink.openForWrite(conf);

            UrlDatum datum = new UrlDatum("http://" + targetDomain);
            datum.addMetaDataValue("crawl-depth", "0");

            writer.add(datum.toTuple());
            writer.close();
        } catch (Exception e) {
            HadoopUtils.safeRemove(fs, _outputDir.getParent());
            throw e;
        }
    }
    
	public void importUrls(boolean debug) throws Exception {
		JobConf conf = HadoopUtils.getDefaultJobConf();
		
		FileSystem fs = _outputDir.getFileSystem(conf);

		try {
            Tap urlSource = new Hfs(new TextLine(), _inputFilename);
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFromTextFunction());
            
            String loopDirName = _outputDir.toUri().toString();
            Tap urlSink = new Hfs(new SequenceFile(UrlDatum.FIELDS.append(MetaData.FIELDS)), loopDirName + "/urls", true);
            
			FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(UrlImporter.class, debug, conf));
			Flow flow = flowConnector.connect(urlSource, urlSink, importPipe);
			flow.complete();
		} catch (Exception e) {
			HadoopUtils.safeRemove(fs, _outputDir.getParent());
			throw e;
		}
	}
}
