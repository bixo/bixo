package bixo;

import java.io.File;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.cascading.MultiSinkTap;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.HttpClientFactory;
import bixo.fetcher.http.IHttpFetcherFactory;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.pipes.FetchPipe;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
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

    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    // TODO KKr - discuss use of context w/Chris.
    @SuppressWarnings("serial")
    private static class CreateUrlFunction extends BaseOperation<String> implements Function<String> {

        public CreateUrlFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<String> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            UrlDatum urlDatum = new UrlDatum(urlAsString, 0, 0, FetchStatusCode.NEVER_FETCHED, null);
            funcCall.getOutputCollector().add(urlDatum.toTuple());
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            File inputFile = null;
            URL path = SimpleCrawlTool.class.getResource("/" + args[0]);
            if (path == null) {
                inputFile = new File(args[0]);
            } else {
                inputFile = new File(path.getFile());
            }

            if (!inputFile.exists()) {
                System.err.println("File not found in filesystem or on classpath: " + args[0]);
                System.exit(-1);
            }

            Tap in = new Hfs(new TextLine(), inputFile.getCanonicalPath());

            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFunction());

            PLDGrouping grouping = new PLDGrouping();
            LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
            IHttpFetcherFactory factory = new HttpClientFactory(10);
            FetchPipe fetchPipe = new FetchPipe(importPipe, grouping, scoring, factory);

            // Create the output, which is a dual file sink tap.
            String outputPath = "build/test-data/SimpleCrawlTool/dual";
            Tap status = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.STATUS_CODE_FIELD)), outputPath + "/status", true);
            Tap content = new Hfs(new TextLine(new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD), new Fields(FetchedDatum.BASE_URL_FIELD, FetchedDatum.CONTENT_FIELD)), outputPath + "/content", true);
            Tap sink = new MultiSinkTap(status, content);

            // Finally we can run it.
            FlowConnector flowConnector = new FlowConnector();
            Flow flow = flowConnector.connect(in, sink, fetchPipe);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running test fetch pipe assembly: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
