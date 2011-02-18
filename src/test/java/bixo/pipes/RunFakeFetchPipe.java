package bixo.pipes;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

import bixo.datum.FetchedDatum;
import bixo.datum.UrlDatum;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.simulation.FakeHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
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

import com.bixolabs.cascading.NullContext;

public class RunFakeFetchPipe {
    private static final Logger LOGGER = Logger.getLogger(RunFakeFetchPipe.class);

    @SuppressWarnings("serial")
    private static class CreateUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlFunction() {
            super(UrlDatum.FIELDS);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
            String urlAsString = funcCall.getArguments().getString("line");
            try {
                URL url = new URL(urlAsString);

                UrlDatum urlDatum = new UrlDatum(url.toString());

                funcCall.getOutputCollector().add(urlDatum.getTuple());
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

            BaseScoreGenerator scorer = new FixedScoreGenerator();
            BaseFetcher fetcher = new FakeHttpFetcher(true, 10);
            FetchPipe fetchPipe = new FetchPipe(importPipe, scorer, fetcher, 1);

            // Create the output, which is a dual file sink tap.
            String outputPath = "build/test/RunFakeFetchPipe/dual";
            Tap status = new Hfs(new TextLine(), outputPath + "/status", true);
            Tap content = new Hfs(new TextLine(null, FetchedDatum.FIELDS), outputPath + "/content", true);
            
            // Finally we can run it.
            FlowConnector flowConnector = new FlowConnector();
            Flow flow = flowConnector.connect(in, FetchPipe.makeSinkMap(status, content), fetchPipe);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running fake fetch pipe assembly: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
