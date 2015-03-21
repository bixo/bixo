/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.examples.webmining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageResult implements Writable {

    public static final String SOURCE_URL_FN = "sourceurl";
    public static final String IMG_URL_FN = "imageurl";
    public static final String DESC_FN = "description";

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
