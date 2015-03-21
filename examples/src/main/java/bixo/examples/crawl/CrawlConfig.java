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
package bixo.examples.crawl;

import bixo.datum.UrlStatus;

public class CrawlConfig {

    public static final String CRAWLDB_SUBDIR_NAME = "crawldb";
    public static final String CONTENT_SUBDIR_NAME = "content";
    public static final String STATUS_SUBDIR_NAME = "status";
    public static final String PARSE_SUBDIR_NAME = "parse";
    public static final String EXTRACTED_TEXT_SUBDIR_NAME = "extracted-text";
    public static final String HTML_SUBDIR_NAME = "html";

    public static final String WEB_ADDRESS = "http://wiki.github.com/bixo/bixo/bixocrawler";
    public static final String EMAIL_ADDRESS = "bixo-dev@yahoogroups.com";

    // Fetcher policy constants
    public static final int CRAWL_STACKSIZE_KB = 128;
    public static final int MAX_CONTENT_SIZE = 128 * 1024;
    public static final long DEFAULT_CRAWL_DELAY = 10 * 1000L; // in millisecond
//    public static final int MAX_CONNECTIONS_PER_HOST = 30;
//    public static final int MAX_REQUESTS_PER_CONNECTION = 10;
//    public static final int MIN_RESPONSE_RATE = 1024;          // Min bytes/second
    
    // Fetcher constants
    public static final int MAX_RETRIES = 2;
    public static final int SOCKET_TIMEOUT = 10 * 1000;
    public static final int CONNECTION_TIMEOUT = 10 * 1000;

    // Misc
    public static final long MILLISECONDS_PER_MINUTE = 60 * 1000L;
    
    // WritableSequenceFile key value field names
    public static final String WRITABLE_SEQ_FILE_KEY_FN = "url";
    public static final String WRITABLE_SEQ_FILE_VALUE_FN = "parsedText";

    
    public static boolean isUnfetchedStatus(UrlStatus status) {
        if (status == UrlStatus.UNFETCHED
            || status == UrlStatus.SKIPPED_DEFERRED
            || status == UrlStatus.SKIPPED_BY_SCORER
            || status == UrlStatus.SKIPPED_BY_SCORE
            || status == UrlStatus.SKIPPED_TIME_LIMIT
            || status == UrlStatus.SKIPPED_INTERRUPTED
            || status == UrlStatus.SKIPPED_INEFFICIENT
            || status == UrlStatus.ABORTED_SLOW_RESPONSE
            || status == UrlStatus.ERROR_IOEXCEPTION) {
            return true;
        }
        return false;
    }
}
