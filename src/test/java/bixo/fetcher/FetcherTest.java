package bixo.fetcher;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import bixo.Constants;
import bixo.urldb.UrlImporter;
import bixo.utils.TimeStampUtil;

public class FetcherTest {

    @Test
    public void testRunFetcher() throws Exception {
        // create url db
        String workingFolder = "build/test-data/FetcherTest/working";

        // we might dont want to regenerate that all the time..
        if (!new File(workingFolder, Constants.URL_DB).exists()) {
            UrlImporter urlImporter = new UrlImporter();
            String inputPath = "src/test-data/top10urls.txt";
            FileUtil.fullyDelete(new File(workingFolder));
            urlImporter.importUrls(inputPath, workingFolder);
        }
        // now fetch those

        FetcherJob fetcher = new FetcherJob();
        String input = workingFolder + "/" + Constants.URL_DB;

        String fetchFolder = workingFolder + "/" + Constants.FETCH + TimeStampUtil.nowWithUnderLine();
        fetcher.run(input, fetchFolder);
    }
}
