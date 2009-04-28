package bixo.content.parser.html;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import bixo.parser.IParse;
import bixo.parser.ParseResult;
import bixo.parser.html.HtmlParser;
import bixo.parser.html.Outlink;
import bixo.tuple.FetchContentTuple;


public class HtmlParserTest extends TestCase {
    
    @Test
    public void testHtlmParsing() throws IOException {
        URL path = HtmlParserTest.class.getResource("/" + "simple-page.html");
        File file = new File(path.getFile());
        byte[] bytes = new byte[(int)file.length()];
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        in.readFully(bytes);
        
        FetchContentTuple content = new FetchContentTuple(path.toExternalForm(), path.toExternalForm(),
                        System.currentTimeMillis(), bytes, "text/html", 0);
        
        HtmlParser parser = new HtmlParser();
        ParseResult parse = parser.getParse(content);
        
        IParse p = parse.get(path.toExternalForm());
        assertNotNull(p);
        
        File parsedTextFile = new File(HtmlParserTest.class.getResource("/" + "simple-page.txt").getFile());
        assertEquals(FileUtils.readFileToString(parsedTextFile, "utf-8"), p.getText());
        
        Outlink[] outlinks = p.getData().getOutlinks();
        assertEquals(10, outlinks.length);
    }
}
