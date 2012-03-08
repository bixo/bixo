/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageResult implements Writable {

    // These field names should match the field names in the solr schema.
    public static final String STUDENT_INDEX_FN = "student";
    public static final String SOURCE_URL_INDEX_FN = "sourceurl";
    public static final String IMG_URL_INDEX_FN = "imageurl";
    public static final String DESC_INDEX_FN = "description";

    private String _sourceUrl;
    private String _imageUrl;
    private String _description;
    
    public PageResult() {
        // Empty constructor for deserialization
    }
    
    public PageResult(String sourceUrl, String imageUrl, String description) {
        _sourceUrl = sourceUrl;
        _imageUrl = imageUrl;
        _description = description;
    }

    public String getSourceUrl() {
        return _sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        _sourceUrl = sourceUrl;
    }

    public String getImageUrl() {
        return _imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        _imageUrl = imageUrl;
    }

    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _sourceUrl = in.readUTF();
        _imageUrl = in.readUTF();
        _description = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_sourceUrl);
        out.writeUTF(_imageUrl);
        out.writeUTF(_description);
    }
    
}
