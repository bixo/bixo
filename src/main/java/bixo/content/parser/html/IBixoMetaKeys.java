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



/**
 * A collection of Nutch internal metadata constants.
 *
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public interface IBixoMetaKeys {
    public static final String ORIGINAL_CHAR_ENCODING_KEY = "bixo.charencoding.original";

    public static final String CHAR_ENCODING_FOR_CONVERSION_KEY = "bixo.charencoding.for_conversion";

    /** Sites may request that search engines don't provide access to cached documents. */
    public static final String CACHING_FORBIDDEN_KEY = "bixo.caching.forbidden";

    /** Show both original forbidden content and summaries (default). */
    public static final String CACHING_FORBIDDEN_NONE = "none";

    /** Don't show either original forbidden content or summaries. */
    public static final String CACHING_FORBIDDEN_ALL = "all";

    /** Don't show original forbidden content, but show summaries. */
    public static final String CACHING_FORBIDDEN_CONTENT = "content";

}
