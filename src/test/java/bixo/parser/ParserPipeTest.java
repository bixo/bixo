package bixo.parser;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.junit.Test;

import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.parser.html.HtmlParserFactory;
import bixo.pipes.ParserPipe;
import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tuple.TupleEntryCollector;

public class ParserPipeTest extends CascadingTestCase {

    @Test
    public void testParserPipe() throws Exception {

        Pipe pipe = new Pipe("parse_source");
        ParserPipe parserPipe = new ParserPipe(pipe, new HtmlParserFactory());
        Lfs in = new Lfs(new SequenceFile(FetchedDatum.FIELDS), "build/test-data/ParserPipeTest/in", true);
        Lfs out = new Lfs(new SequenceFile(ParsedDatum.FIELDS), "build/test-data/ParserPipeTest/out", true);

        TupleEntryCollector write = in.openForWrite(new JobConf());

        ArchiveReader archiveReader = ArchiveReaderFactory.get("src/test-data/someHtml.arc");
        Iterator<ArchiveRecord> iterator = archiveReader.iterator();
        int max = 300;
        int count = 0;
        int validRecords = 0;
        while (count++ < max && iterator.hasNext()) {
            ArchiveRecord archiveRecord = (ArchiveRecord) iterator.next();
            ArchiveRecordHeader header = archiveRecord.getHeader();
            String url = header.getUrl();

            String protocol = "";
            try {
                protocol = new URL(url).getProtocol();
            } catch (MalformedURLException e) {
                // Ignore and skip
            }

            if (protocol.equals("http")) {
                validRecords += 1;
                int contentOffset = header.getContentBegin();
                long totalLength = header.getLength();
                int contentLength = (int) totalLength - contentOffset;

                archiveRecord.skip(contentOffset);
                byte[] content = new byte[contentLength];
                archiveRecord.read(content);

                String mimetype = header.getMimetype();
                FetchedDatum contentTuple = new FetchedDatum(FetchStatusCode.FETCHED, url, url, System.currentTimeMillis(), new BytesWritable(content), mimetype, 0, null);
                write.add(contentTuple.toTuple());
            }
        }

        write.close();
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, out, parserPipe);
        flow.complete();
        validateLength(flow, validRecords);

    }

}
