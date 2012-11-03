package com.finderbots.miner2;

import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlNormalizer;
import bixo.urls.BaseUrlValidator;
import bixo.urls.SimpleUrlNormalizer;
import bixo.urls.SimpleUrlValidator;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public class UrlImporter {
    private static final Logger LOGGER = Logger.getLogger(UrlImporter.class);

    @SuppressWarnings("serial")
    private static class CreateUrlFromTextFunction extends BaseOperation<NullContext> implements Function<NullContext> {
        private BaseUrlNormalizer _normalizer;
        private BaseUrlValidator _validator;

        public CreateUrlFromTextFunction(BaseUrlNormalizer normalizer, BaseUrlValidator validator) {
            super(CrawlDbDatum.FIELDS);
            _normalizer = normalizer;
            _validator = validator;
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            if (urlAsString.length() > 0) {
                // Normalize and Validate the URL
                urlAsString = _normalizer.normalize(urlAsString);
                if (_validator.isValid(urlAsString)) {
                    CrawlDbDatum crawlDatum = new CrawlDbDatum(urlAsString, 0, 0, UrlStatus.UNFETCHED, 0);
                    funcCall.getOutputCollector().add(crawlDatum.getTuple());
                }
            } else {
                LOGGER.error("Malformed url in import list: " + urlAsString);
            }
        }
    }

    private Path _inputFilePath;
    private Path _destDirPath;

    /**
     * UrlImporter is a simple class that can be used to import a list of urls into the
     * crawl db
     *
     * @param inputFilePath : the input text file containing the urls to be imported
     * @param desDirPath : the destination path at which to create the crawl db.
     */
    public UrlImporter(Path inputFilePath, Path desDirPath) {
        _inputFilePath = inputFilePath;
        _destDirPath = desDirPath;
    }

    public void importUrls(boolean debug) throws Exception {
        JobConf conf = HadoopUtils.getDefaultJobConf();

        FileSystem fs = _destDirPath.getFileSystem(conf);

        try {
            Tap urlSource = new Hfs(new TextLine(), _inputFilePath.toString());
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFromTextFunction(new SimpleUrlNormalizer(), new SimpleUrlValidator()));

            Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), _destDirPath.toString(), true);

            FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(UrlImporter.class, debug, conf));
            Flow flow = flowConnector.connect(urlSource, urlSink, importPipe);
            flow.complete();
        } catch (Exception e) {
            HadoopUtils.safeRemove(fs, _destDirPath);
            throw e;
        }
    }

}
