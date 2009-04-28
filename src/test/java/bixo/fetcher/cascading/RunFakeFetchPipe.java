package bixo.fetcher.cascading;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.IConstants;
import bixo.cascading.MultiSinkTap;
import bixo.fetcher.FakeHttpFetcherFactory;
import bixo.fetcher.IHttpFetcherFactory;
import bixo.fetcher.beans.FetchStatusCode;
import bixo.fetcher.util.LastFetchScoreGenerator;
import bixo.fetcher.util.PLDGrouping;
import bixo.tuple.BaseDatum;
import bixo.tuple.UrlDatum;
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
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class RunFakeFetchPipe {
    private static final Logger LOGGER = Logger.getLogger(RunFakeFetchPipe.class);
    
    private static final long TEN_DAYS = 1000L * 60 * 60 * 24 * 10;

    // TODO KKr - discuss use of context w/Chris.
    @SuppressWarnings("serial")
    private static class CreateUrlFunction extends BaseOperation<String> implements Function<String> {
        
        public CreateUrlFunction() {
            super(BaseDatum.FIELDS);
        }
        
        @Override
        public void operate(FlowProcess process, FunctionCall<String> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            try {
                URL url = new URL(urlAsString);
                
                UrlDatum urlDatum = new UrlDatum();
                urlDatum.setUrl(url);
                urlDatum.setLastFetched(0);
                urlDatum.setLastUpdated(0);
                urlDatum.setLastStatus(FetchStatusCode.NEVER_FETCHED);
                
                funcCall.getOutputCollector().add(urlDatum.toTuple());
            } catch (MalformedURLException e) {
                LOGGER.warn("Invalid URL: " + urlAsString);
                // throw new RuntimeException("Invalid URL: " + urlAsString, e);
            }
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            URL path = RunFakeFetchPipe.class.getResource("/" + args[0]);
            if (path == null) {
                System.err.println("File not found on classpath: " + args[0]);
                System.exit(-1);
            }
            
            File inputFile = new File(path.getFile());
            Tap in = new Lfs(new TextLine(), inputFile.getCanonicalPath());
            
            Pipe importPipe = new Each("url importer", new Fields("line"), new CreateUrlFunction());
            
            PLDGrouping grouping = new PLDGrouping();
            LastFetchScoreGenerator scoring = new LastFetchScoreGenerator(System.currentTimeMillis(), TEN_DAYS);
            IHttpFetcherFactory factory = new FakeHttpFetcherFactory(true, 10);
            FetchPipe fetchPipe = new FetchPipe(importPipe, grouping, scoring, factory);
            
            // Create the output, which is a dual file sink tap.
            String outputPath = "build/test-data/RunFakeFetchPipe/dual";
            Tap status = new Hfs(new TextLine(new Fields(IConstants.URL, IConstants.FETCH_STATUS), new Fields(IConstants.URL, IConstants.FETCH_STATUS)), outputPath + "/status", true);
            Tap content = new Hfs(new TextLine(new Fields(IConstants.URL, IConstants.CONTENT), new Fields(IConstants.URL, IConstants.FETCH_CONTENT)), outputPath + "/content", true);
            Tap sink = new MultiSinkTap(status, content);

            // Finally we can run it.
            FlowConnector flowConnector = new FlowConnector();
            Flow flow = flowConnector.connect(in, sink, fetchPipe);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running fake fetch pipe assembly: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
