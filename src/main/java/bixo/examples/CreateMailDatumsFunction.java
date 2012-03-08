package bixo.examples;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import bixo.datum.FetchedDatum;
import bixo.datum.MailDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class CreateMailDatumsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateMailDatumsFunction.class);

    private int _numCreated;

    public CreateMailDatumsFunction() {
        super(MailDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting creation of products from page");
        _numCreated = 0;
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending creation of products");
        LOGGER.info(String.format("Created %d products", _numCreated));
    }

    public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        FetchedDatum datum = new FetchedDatum(funcCall.getArguments());

        String content = new String(datum.getContentBytes());
        Document doc = Jsoup.parse(content);
        Elements elements = doc.select("a");

        TupleEntryCollector collector = funcCall.getOutputCollector();

        // do the parsing and emit as many MailDatum as are present
        for (Element element : elements) {
            if (element.attr("href").startsWith("mailto:")) {
                MailDatum mailDatum = new MailDatum();
                String name = element.text();
                if (name == null || name.isEmpty() || name.trim().isEmpty()) {
                    name = "*** missing name ***";
                }
                mailDatum.setName(name);
                mailDatum.setEmail(element.attr("href").substring(7));
                collector.add(mailDatum.getTuple());
                _numCreated++;
            }
        }
    }
}
