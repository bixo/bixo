package bixo.examples;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import bixo.datum.FetchedDatum;
import bixo.datum.ProductDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class CreateProductDatumsFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(CreateProductDatumsFunction.class);

    private int _numCreated;

    public CreateProductDatumsFunction() {
        super(ProductDatum.FIELDS);
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
        Elements elements = doc.select("div.dis_content_img");

        TupleEntryCollector collector = funcCall.getOutputCollector();

        // do the parsing and emit as many ProductDatum as are present
        for (Element element : elements) {
            ProductDatum productDatum = new ProductDatum();
            productDatum.setDepartment("shoes");
            productDatum.setCategory("boots");
            try {
                Document fragment = Jsoup.parseBodyFragment(element.html());
                productDatum.setBrand(fragment.select("span.productlist_marque").text());
                productDatum.setModel(fragment.select("span.productlist_name").text());
                productDatum.setPrice(fragment.select("span.productlist_prix").text());
            } catch (Exception e) {
                LOGGER.error("Error parsing html fragment: " + element.html());
                e.printStackTrace();
            }
            collector.add(productDatum.getTuple());
            _numCreated++;
        }
    }
}
