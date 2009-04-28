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
package bixo.urldb;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.IConstants;
import bixo.tuple.BaseDatum;
import cascading.CascadingTestCase;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tuple.TupleEntryIterator;

public class UrlImporterTest extends CascadingTestCase {

    @Test
    public void testImportFiles() throws Exception {
        UrlImporter urlImporter = new UrlImporter();
        String inputPath = "src/test-data/top10urls.txt";
        String workingFolder = "build/test-data/UrlImporterTest/working";
        FileUtil.fullyDelete(new File(workingFolder));
        urlImporter.importUrls(inputPath, workingFolder);

        Hfs hfs = new Hfs(new SequenceFile(BaseDatum.FIELDS), workingFolder + "/" + IConstants.URL_DB);
        TupleEntryIterator tupleEntryIterator = hfs.openForRead(new JobConf());
        validateLength(tupleEntryIterator, 10);

        urlImporter.importUrls(inputPath, workingFolder);
        // should be still only 10
        hfs = new Hfs(new SequenceFile(BaseDatum.FIELDS), workingFolder + "/" + IConstants.URL_DB);
        tupleEntryIterator = hfs.openForRead(new JobConf());
        validateLength(tupleEntryIterator, 10);
    }
}
