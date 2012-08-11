package bixo.examples.crawl;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import org.junit.Test;

import bixo.datum.UrlDatum;

public class RegexUrlFilterTest {

    
    @Test
    public void testFilter() {
        String[] patterns = {
                        "-(?i)\\.(jpg|gif|bmp)$",
                        "-(?i)^http://([a-z0-9]*\\.)*foo.com/bar/",
                        "+(?i)^http://([a-z0-9]*\\.)*foo.com/accept",
        };

        RegexUrlFilter filter = new RegexUrlFilter(patterns);
        UrlDatum datum = new UrlDatum("http://my.foo.com/accept");
        assertFalse(filter.isRemove(datum));

        datum.setUrl("http://my.foo.com/bar/");
        assertTrue(filter.isRemove(datum));

        datum.setUrl("http://my.foo.com/accept/shouldstillberemoved.gif");
        assertTrue(filter.isRemove(datum));

    }
    
    @Test
    public void testDefault() throws IOException {
        List<String> defaultUrlFilterPatterns = RegexUrlFilter.getDefaultUrlFilterPatterns();
        String domainPatterStr = "+(?i)^(http|https)://([a-z0-9]*\\.)*" + "foo.com";
        defaultUrlFilterPatterns.add(domainPatterStr);

        RegexUrlFilter filter = new RegexUrlFilter(defaultUrlFilterPatterns.toArray(new String[defaultUrlFilterPatterns.size()]));
        UrlDatum datum = new UrlDatum("http://my.foo.com/");
        assertFalse(filter.isRemove(datum));
   
        datum.setUrl("http://my.foo.com/accept/shouldberemoved.exe");
        assertTrue(filter.isRemove(datum));

    }
    
    
    @Test (expected=PatternSyntaxException.class)
    public void testInvalidPatternsConstructor() {
        String[] patterns = {"-(?k)\\.(jpg|gif|bmp)$"};
        RegexUrlFilter filter = new RegexUrlFilter(patterns);
        System.out.println(filter.toString());
    }
}
