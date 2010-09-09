package bixo.robots;

import java.net.URL;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.IOFetchException;
import bixo.exceptions.RedirectFetchException;
import bixo.fetcher.http.HttpFetcher;
import bixo.fetcher.http.SimpleHttpFetcher;

public class RobotUtils {
    private static final Logger LOGGER = Logger.getLogger(RobotUtils.class);
    
    // Some robots.txt files are > 64K, amazingly enough.
    private static final int MAX_ROBOTS_SIZE = 128 * 1024;

    // subdomain.domain.com can direct to domain.com, so if we're simultaneously fetching
    // a bunch of robots from subdomains that redirect, we'll exceed the default limit.
    private static final int MAX_CONNECTIONS_PER_HOST = 20;
    
    // Crank down default values when fetching robots.txt, as this should be super
    // fast to get back.
    private static final int ROBOTS_CONNECTION_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_SOCKET_TIMEOUT = 10 * 1000;
    private static final int ROBOTS_RETRY_COUNT = 2;

    // TODO KKr - set up min response rate, use it with max size to calc max
    // time for valid download, use it for COMMAND_TIMEOUT
    
    // Amount of time we'll wait for pending tasks to finish up. This is roughly equal
    // to the max amount of time it might take to fetch a robots.txt file (excluding
    // download time, which we could add).
    // FUTURE KKr - add in time to do the download.
    private static final long MAX_FETCH_TIME = (ROBOTS_CONNECTION_TIMEOUT + ROBOTS_SOCKET_TIMEOUT) * ROBOTS_RETRY_COUNT;

    public static HttpFetcher createFetcher(UserAgent userAgent, int maxThreads) {
        // TODO KKr - add static createRobotsFetcher method somewhere that
        // I can use here, and also in SimpleGroupingKeyGenerator
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxContentSize(MAX_ROBOTS_SIZE);
        policy.setMaxConnectionsPerHost(MAX_CONNECTIONS_PER_HOST);
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(maxThreads, policy, userAgent);
        fetcher.setMaxRetryCount(ROBOTS_RETRY_COUNT);
        fetcher.setConnectionTimeout(ROBOTS_CONNECTION_TIMEOUT);
        fetcher.setSocketTimeout(ROBOTS_SOCKET_TIMEOUT);
        
        return fetcher;
    }
    
    public static long getMaxFetchTime() {
        return MAX_FETCH_TIME;
    }

    /**
     * Externally visible, static method for use in tools and for testing.
     * Fetch the indicated robots.txt file, parse it, and generate rules.
     * 
     * @param fetcher Fetcher for downloading robots.txt file
     * @param robotsUrl URL to robots.txt file
     * @return Robot rules
     */
    public static RobotRules getRobotRules(HttpFetcher fetcher, RobotRulesParser parser, URL robotsUrl) {
        
        try {
            String urlToFetch = robotsUrl.toExternalForm();
            ScoredUrlDatum scoredUrl = new ScoredUrlDatum(urlToFetch);
            FetchedDatum result = fetcher.get(scoredUrl);

            // HACK! DANGER! Some sites will redirect the request to the top-level domain
            // page, without returning a 404. So look for a response which has a redirect,
            // and the fetched content is not plain text, and assume it's one of these...
            // which is the same as not having a robots.txt file.
            
            String contentType = result.getContentType();
            boolean isPlainText = (contentType != null) && (contentType.startsWith("text/plain"));
            if ((result.getNumRedirects() > 0) && !isPlainText) {
                return parser.failedFetch(HttpStatus.SC_GONE);
            }
            
            return parser.parseContent(urlToFetch, result.getContentBytes(), result.getContentType(), 
                            fetcher.getUserAgent().getAgentName());
        } catch (HttpFetchException e) {
            return parser.failedFetch(e.getHttpStatus());
        } catch (IOFetchException e) {
            return parser.failedFetch(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        } catch (RedirectFetchException e) {
            // Other sites will have circular redirects, so treat this as a missing robots.txt
            return parser.failedFetch(HttpStatus.SC_GONE);
        } catch (Exception e) {
            LOGGER.error("Unexpected exception fetching robots.txt: " + robotsUrl, e);
            return parser.failedFetch(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
    }

}
