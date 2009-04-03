package bixo.urldb;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import bixo.Constants;
import bixo.items.UrlItem;
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

        Hfs hfs = new Hfs(new SequenceFile(UrlItem.FIELDS), workingFolder + "/" + Constants.URL_DB);
        TupleEntryIterator tupleEntryIterator = hfs.openForRead(new JobConf());
        validateLength(tupleEntryIterator, 10);

        urlImporter.importUrls(inputPath, workingFolder);
        // should be still only 10
        hfs = new Hfs(new SequenceFile(UrlItem.FIELDS), workingFolder + "/" + Constants.URL_DB);
        tupleEntryIterator = hfs.openForRead(new JobConf());
        validateLength(tupleEntryIterator, 10);
    }
}
