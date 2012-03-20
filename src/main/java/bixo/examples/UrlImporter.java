package bixo.examples;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import bixo.datum.UrlStatus;
import bixo.urls.SimpleUrlNormalizer;
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

import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.NullContext;

public class UrlImporter {

    public static class CreateUrlFromTextFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlFromTextFunction() {
            super(CrawlDbDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String url = funcCall.getArguments().getString("line");
            // Do any manipulation of the text line - you may want to
            // normalize+validate urls, domains
            SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
            CrawlDbDatum datum = new CrawlDbDatum();
            datum.setUrl(normalizer.normalize(url));
            datum.setLastStatus(UrlStatus.UNFETCHED);
            funcCall.getOutputCollector().add(datum.getTuple());
        }
    }

    private Path _inputFilePath;
    private Path _outputDirPath;

    public UrlImporter(Path inputFilePath, Path outputDirPath) {
        _inputFilePath = inputFilePath;
        _outputDirPath = outputDirPath;
    }

    public void importUrls(boolean debug) throws Exception {
        JobConf conf = HadoopUtils.getDefaultJobConf();

        FileSystem fs = _outputDirPath.getFileSystem(conf);

        try {
            Tap urlSource = new Hfs(new TextLine(), _inputFilePath.toString());
     
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFromTextFunction());

            Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), _outputDirPath.toUri().toString(), true);

            FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(UrlImporter.class, debug, conf));
            Flow flow = flowConnector.connect(urlSource, urlSink, importPipe);
            flow.complete();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
