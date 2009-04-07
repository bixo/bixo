/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
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
