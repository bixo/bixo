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
package bixo.datum;

public enum UrlStatus {
    UNFETCHED,                  // Never processed.
    
    // Not fetched due to pre-fetch operations
    SKIPPED_BLOCKED,            // Blocked by robots.txt
    SKIPPED_UNKNOWN_HOST,       // Hostname couldn't be resolved to IP address
    SKIPPED_INVALID_URL,        // URL invalid
    SKIPPED_DEFERRED,           // Deferred because robots.txt couldn't be processed.
    SKIPPED_BY_SCORER,          // Skipped explicitly by scorer or grouper
    SKIPPED_BY_SCORE,           // Skipped because score wasn't high enough
    SKIPPED_TIME_LIMIT,         // Ran out of time (also during fetch)
    SKIPPED_FILTERED,           // Filtered out during processing
    SKIPPED_PER_SERVER_LIMIT,   // Too many URLs per server
    
    // Not fetched due to mid-fetch issues
    SKIPPED_INTERRUPTED,        // Fetch process was interrupted.
    SKIPPED_INEFFICIENT,        // Skipped because we were blocked on domain (running with skip-blocked fetch policy)
    ABORTED_SLOW_RESPONSE,      // Response rate < min set in fetch policy
    ABORTED_FETCHER_POLICY,     // Mime type != policy's valid types, content length exceeds policy's max length, etc.
    
    // Not fetched during fetch operation, due to HTTP status code error
    HTTP_REDIRECTION_ERROR,
    HTTP_TOO_MANY_REDIRECTS,
    HTTP_MOVED_PERMANENTLY,
    
    HTTP_CLIENT_ERROR,
    HTTP_UNAUTHORIZED,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    HTTP_GONE,
        
    HTTP_SERVER_ERROR,

    // Not fetched during fetch operation, due to error
    ERROR_INVALID_URL,
    ERROR_IOEXCEPTION,
    
    FETCHED;    // Successfully fetched

}
