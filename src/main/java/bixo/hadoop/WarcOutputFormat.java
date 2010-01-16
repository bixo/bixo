package bixo.hadoop;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.archive.io.warc.WARCWriter;
import org.archive.util.ArchiveUtils;

import bixo.datum.FetchedDatum;
import bixo.datum.HttpHeaders;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

public class WarcOutputFormat extends FileOutputFormat<Tuple, Tuple> {
    public static final String WARC_METADATA_FIELDS_KEY = "WarcOutputFormat-metadata-fields-key";
    
    protected static class WarcRecordWriter implements RecordWriter<Tuple, Tuple> {

        private static final String WARC_RESPONSE_CONTENT_TYPE = "application/http; msgtype=response";
        
        private Fields _metadataFields;
        private WARCWriter _warcWriter;
        
        public WarcRecordWriter(DataOutputStream out, Fields metadataFields) throws IOException {
            _metadataFields = metadataFields;
            
            _warcWriter = new WARCWriter(new AtomicInteger(), out, null, false,
                            ArchiveUtils.get14DigitDate(), new ArrayList<String>());
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            _warcWriter.close();
        }

        @Override
        public void write(Tuple key, Tuple value) throws IOException {
            // We ignore the key. The <value> must be for a FetchedDatum.
            FetchedDatum datum = new FetchedDatum(value, _metadataFields);
            
            // Now we need to write out a new response record.
            byte[] headerBytes = headersAsBytes(datum.getHeaders());
            byte[] contentBytes = datum.getContentBytes();
            int totalLength = headerBytes.length + 1 + contentBytes.length;
            
            // TODO KKr - create input stream that combines the header & content bytes (plus LF in middle)
            _warcWriter.writeResponseRecord(datum.getFetchedUrl(), ArchiveUtils.get14DigitDate(datum.getFetchTime()), WARC_RESPONSE_CONTENT_TYPE, 
                            WARCWriter.getRecordID(), null, new ByteArrayInputStream(datum.getContentBytes()), totalLength);
        }
        
        private static byte[] headersAsBytes(HttpHeaders headers) {
            // TODO KKr - return headers as a byte array.
            return new byte[0];
        }
    }

    @Override
    public RecordWriter<Tuple, Tuple> getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException {
        Fields metadataFields = (Fields)Util.deserializeBase64(conf.get(WARC_METADATA_FIELDS_KEY));
        
        boolean isCompressed = getCompressOutput(conf);
        if (!isCompressed) {
            Path file = FileOutputFormat.getTaskOutputPath(conf, name);
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new WarcRecordWriter(fileOut, metadataFields);
        } else {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(conf, GzipCodec.class);
            CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
            // build the filename including the extension
            Path file = FileOutputFormat.getTaskOutputPath(conf, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new WarcRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), metadataFields);
        }
    }
}
