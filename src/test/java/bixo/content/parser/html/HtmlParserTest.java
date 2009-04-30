package bixo.content.parser.html;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;
import org.junit.Test;

import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.parser.IParse;
import bixo.parser.IParser;
import bixo.parser.IParserFactory;
import bixo.parser.ParseResult;
import bixo.parser.html.HtmlParser;
import bixo.parser.html.HtmlParserFactory;

public class HtmlParserTest {

    private FetchedDatum makeFetchedDat(URL path) throws IOException {
        File file = new File(path.getFile());
        byte[] bytes = new byte[(int) file.length()];
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        in.readFully(bytes);

        FetchedDatum fetchedDatum = new FetchedDatum(FetchStatusCode.FETCHED, path.toExternalForm().toString(), path.toExternalForm().toString(), System.currentTimeMillis(), new BytesWritable(bytes),
                        "text/html", 0, null);

        return fetchedDatum;
    }

    @Test
    public void testHtlmParsing() throws IOException {
        URL path = HtmlParserTest.class.getResource("/" + "simple-page.html");

        HtmlParser parser = new HtmlParser();
        FetchedDatum content = makeFetchedDat(path);
        ParseResult parse = parser.getParse(content);

        IParse p = parse.get(path.toExternalForm());
        Assert.assertNotNull(p);

        File parsedTextFile = new File(HtmlParserTest.class.getResource("/" + "simple-page.txt").getFile());
        Assert.assertEquals(FileUtils.readFileToString(parsedTextFile, "utf-8"), p.getText());

        Outlink[] outlinks = p.getData().getOutlinks();
        Assert.assertEquals(10, outlinks.length);
    }

    @Test
    public void testIParserInterface() throws IOException {
        IParserFactory factory = new HtmlParserFactory();
        IParser parser = factory.newParser();

        URL path = HtmlParserTest.class.getResource("/" + "simple-page.html");
        FetchedDatum content = makeFetchedDat(path);

        ParsedDatum result = parser.parse(content);
        File parsedTextFile = new File(HtmlParserTest.class.getResource("/" + "simple-page.txt").getFile());
        Assert.assertEquals(FileUtils.readFileToString(parsedTextFile, "utf-8"), result.getParsedText());
        Assert.assertEquals(10, result.getOulinks().length);
    }
}
