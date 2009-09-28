package bixo.parser.html;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;
import org.junit.Test;

import bixo.datum.FetchedDatum;
import bixo.datum.Outlink;
import bixo.datum.ParsedDatum;
import bixo.parser.IParse;
import bixo.parser.IParser;
import bixo.parser.ParseResult;

public class HtmlParserTest {

    private FetchedDatum makeFetchedDat(URL path) throws IOException {
        File file = new File(path.getFile());
        byte[] bytes = new byte[(int) file.length()];
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        in.readFully(bytes);

        String url = path.toExternalForm().toString();
        FetchedDatum fetchedDatum = new FetchedDatum(url, url, System.currentTimeMillis(), null, new BytesWritable(bytes), "text/html", 0, null);
        return fetchedDatum;
    }

    @Test
    public void testHtlmParsing() throws IOException {
        URL path = HtmlParserTest.class.getResource("/" + "simple-page.html");

        NutchHtmlParser parser = new NutchHtmlParser("windows-1252", IBixoMetaKeys.CACHING_FORBIDDEN_CONTENT);
        FetchedDatum content = makeFetchedDat(path);
        ParseResult parse = parser.getParse(content);

        IParse p = parse.get(path.toExternalForm());
        Assert.assertNotNull(p);

        File parsedTextFile = new File(HtmlParserTest.class.getResource("/" + "simple-page.txt").getFile());
        Assert.assertEquals(FileUtils.readFileToString(parsedTextFile, "utf-8"), p.getText());

        Outlink[] outlinks = p.getData().getOutlinks();
        Assert.assertEquals(10, outlinks.length);
        
        Assert.assertEquals("TransPac Software", p.getData().getTitle());
    }

    @Test
    public void testIParserInterface() throws IOException {
        IParser parser = new NutchHtmlParser("windows-1252", IBixoMetaKeys.CACHING_FORBIDDEN_CONTENT);

        URL path = HtmlParserTest.class.getResource("/" + "simple-page.html");
        FetchedDatum content = makeFetchedDat(path);

        ParsedDatum result = parser.parse(content);
        File parsedTextFile = new File(HtmlParserTest.class.getResource("/" + "simple-page.txt").getFile());
        Assert.assertEquals(FileUtils.readFileToString(parsedTextFile, "utf-8"), result.getParsedText());
        Assert.assertEquals(10, result.getOutlinks().length);
    }
}
