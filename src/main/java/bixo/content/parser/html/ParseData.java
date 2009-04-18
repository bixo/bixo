/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bixo.content.parser.html;

import java.util.Arrays;


/** Data extracted from a page's content.
 * @see IParse#getData()
 */
public final class ParseData {
    public static final String DIR_NAME = "parse_data";

    private final static byte VERSION = 5;

    private String title;
    private Outlink[] outlinks;
    private Metadata contentMeta;
    private Metadata parseMeta;
    private ParseStatus status;
    private byte version = VERSION;

    public ParseData() {
        contentMeta = new Metadata();
        parseMeta = new Metadata();
    }

    public ParseData(ParseStatus status, String title, Outlink[] outlinks,
                    Metadata contentMeta) {
        this(status, title, outlinks, contentMeta, new Metadata());
    }

    public ParseData(ParseStatus status, String title, Outlink[] outlinks,
                    Metadata contentMeta, Metadata parseMeta) {
        this.status = status;
        this.title = title;
        this.outlinks = outlinks;
        this.contentMeta = contentMeta;
        this.parseMeta = parseMeta;
    }

    //
    // Accessor methods
    //

    /** The status of parsing the page. */
    public ParseStatus getStatus() { return status; }

    /** The title of the page. */
    public String getTitle() { return title; }

    /** The outlinks of the page. */
    public Outlink[] getOutlinks() { return outlinks; }

    /** The original Metadata retrieved from content */
    public Metadata getContentMeta() { return contentMeta; }

    /**
     * Other content properties.
     * This is the place to find format-specific properties.
     * Different parser implementations for different content types will populate
     * this differently.
     */
    public Metadata getParseMeta() { return parseMeta; }

    public void setParseMeta(Metadata parseMeta) {
        this.parseMeta = parseMeta;
    }

    /**
     * Get a metadata single value.
     * This method first looks for the metadata value in the parse metadata. If no
     * value is found it the looks for the metadata in the content metadata.
     * @see #getContentMeta()
     * @see #getParseMeta()
     */
    public String getMeta(String name) {
        String value = parseMeta.get(name);
        if (value == null) {
            value = contentMeta.get(name);
        }
        return value;
    }

    //
    // other methods
    //

    public boolean equals(Object o) {
        if (!(o instanceof ParseData))
            return false;
        ParseData other = (ParseData)o;
        return
        this.status.equals(other.status) &&
        this.title.equals(other.title) &&
        Arrays.equals(this.outlinks, other.outlinks) &&
        this.contentMeta.equals(other.contentMeta) &&
        this.parseMeta.equals(other.parseMeta);
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();

        buffer.append("Version: " + version + "\n" );
        buffer.append("Status: " + status + "\n" );
        buffer.append("Title: " + title + "\n" );

        if (outlinks != null) {
            buffer.append("Outlinks: " + outlinks.length + "\n" );
            for (int i = 0; i < outlinks.length; i++) {
                buffer.append("  outlink: " + outlinks[i] + "\n");
            }
        }

        buffer.append("Content Metadata: " + contentMeta + "\n" );
        buffer.append("Parse Metadata: " + parseMeta + "\n" );

        return buffer.toString();
    }

}
