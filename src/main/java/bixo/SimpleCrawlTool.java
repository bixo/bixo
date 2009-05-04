package bixo;

import java.io.File;
import java.net.URL;

import bixo.cascading.MultiSinkTap;
import bixo.cascading.NullContext;
import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.http.HttpClientFetcher;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.pipes.FetchPipe;
import bixo.urldb.IUrlNormalizer;
import bixo.urldb.UrlNormalizer;
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
    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    @SuppressWarnings("serial")
    private static class CreateUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
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

            IUrlNormalizer urlNormalizer = new UrlNormalizer();
            PLDGrouping grouping = new PLDGrouping();
            LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
            IHttpFetcher fetcher = new HttpClientFetcher(10);
            FetchPipe fetchPipe = new FetchPipe(importPipe, urlNormalizer, grouping, scoring, fetcher);

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
