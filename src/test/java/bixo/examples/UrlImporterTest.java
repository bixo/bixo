package bixo.examples;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class UrlImporterTest {

    @Test
    public void test() throws Exception {
        UrlImporter ui = new UrlImporter(new Path("src/test/resources/site-list.txt"), new Path("build/test/site-list"));
        ui.importUrls(true);
    }

}
