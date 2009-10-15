package bixo.fetcher.http;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.IOFetchException;

public class SimpleRobotRules implements IRobotRules {
    private static final Logger LOGGER = Logger.getLogger(SimpleRobotRules.class);

    // These must be lower-case, for matching.
    private static final String USER_AGENT_FIELD = "user-agent:";
    private static final String DISALLOW_FIELD = "disallow:";
    private static final String ALLOW_FIELD = "allow:";
    private static final String CRAWL_DELAY_FIELD = "crawl-delay:";
    private static final String SITEMAP_FIELD = "sitemap:";
    
    // If true, then there was a problem getting/parsing robots.txt, and the crawler
    // should defer visits until some later time.
    private boolean _deferVisits = false;
    
    protected RobotRules _robotRules;
    
    /**
     * Single rule that maps from a path prefix to an allow flag.
     */
    protected class RobotRule {
        String _prefix;
        boolean _allow;
        
        public RobotRule(String prefix, boolean allow) {
            _prefix = prefix;
            _allow = allow;
        }
    }
    
    /**
     * Result from parsing a single robots.txt file - which means we
     * get a set of rules, and a crawl-delay.
     */
    protected class RobotRules {
        ArrayList<RobotRule> _rules = new ArrayList<RobotRule>();
        private long _crawlDelay = UNSET_CRAWL_DELAY;

        private void clearRules() {
            _rules.clear();
        }

        private void addRule(String prefix, boolean allow) {
            // Convert old-style case of disallow: <nothing>
            // into new allow: <nothing>.
            if (!allow && (prefix.length() == 0)) {
                allow = true;
            }
            
            _rules.add(new RobotRule(prefix, allow));
        }

        public long getCrawlDelay() {
            return _crawlDelay;
        }

        public void setCrawlDelay(long crawlDelay) {
            _crawlDelay = crawlDelay;
        }

        // TODO KKr - make sure paths are sorted from longest to shortest,
        // to implement longest match
        public boolean isAllowed(String path) {
            for (RobotRule rule : _rules) {
                if (path.startsWith(rule._prefix)) {
                    return rule._allow;
                }
            }

            return true;
        }

        /**
         * Is our ruleset set up to allow all access? Check for special case
         * we set up, with one rule, "/", allowed.
         * 
         * @return true if all URLs are allowed.
         */
        public boolean allowAll() {
            if (_rules.size() == 1) {
                RobotRule rule = _rules.get(0);
                return rule._allow; // If we have a single allow, then all are allowed.
            } else {
                return false;
            }
        }

        /**
         * Is our ruleset set up to disallow all access? Check for special case
         * we set up with one rule, "/", not allowed.
         * 
         * @return true if no URLs are allowed.
         */
        public boolean allowNone() {
            if (_rules.size() == 1) {
                RobotRule rule = _rules.get(0);
                return !rule._allow && rule._prefix.equals("/");
            } else {
                return false;
            }
        }
    }
    
    
    protected SimpleRobotRules() {
        // Hide default constructor
    }
    
    public SimpleRobotRules(int httpStatus) {
        createAllOrNone(httpStatus);
    }
    
    // TODO KKr - get rid of this version, and add a generic one (robot name, URL) that uses
    // Java URL code to fetch it, as well as a version that takes an output stream (e.g. from
    // HttpClient) and a version that takes a String with the content.
    public SimpleRobotRules(String robotName, SimpleHttpFetcher fetcher, String url) {
        try {
            URL realUrl = new URL(url);
            String urlToFetch = new URL(realUrl, "/robots.txt").toExternalForm();

            ScoredUrlDatum scoredUrl = new ScoredUrlDatum(urlToFetch);
            FetchedDatum result = fetcher.get(scoredUrl);
            
            // HACK! DANGER! Some sites will redirect the request to the top-level domain
            // page, without returning a 404. So if we have a redirect, and the normalized
            // redirect URL is the same as the domain, then treat it like a 404...otherwise
            // our robots.txt parser will barf, and we treat that as a "deferred" case.
            parseRules(robotName, urlToFetch, result.getContent().getBytes());
        } catch (MalformedURLException e) {
            LOGGER.error("Invalid URL: " + url);
            createAllOrNone(false);
        } catch (HttpFetchException e) {
            createAllOrNone(e.getHttpStatus());
        } catch (IOFetchException e) {
            createAllOrNone(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.error("Unexpected exception fetching robots.txt: " + url);
            createAllOrNone(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
    }
    
    public SimpleRobotRules(String robotName, byte[] robotsContent) {
        parseRules(robotName, "url", robotsContent);
    }
    
    protected void createAllOrNone(boolean allowAll) {
        _robotRules = new RobotRules();
        _robotRules.addRule("/", allowAll);
    }
    
    protected void createAllOrNone(int httpStatus) {
        if ((httpStatus >= 200) && (httpStatus < 300)) {
            throw new IllegalStateException("Can't use status code constructor with 2xx response");
        } else if ((httpStatus >= 300) && (httpStatus < 400)) {
            // Should only happen if we're getting endless redirects (more than our follow limit), so
            // treat it as a temporary failure.
            _deferVisits = true;
            createAllOrNone(false);
        } else if (httpStatus == HttpStatus.SC_NOT_FOUND) {
            createAllOrNone(true);
        } else if ((httpStatus == HttpStatus.SC_FORBIDDEN) || (httpStatus == HttpStatus.SC_UNAUTHORIZED)) {
            createAllOrNone(false);
        } else {
            // Treat all other status codes as a temporary failure.
            _deferVisits = true;
            createAllOrNone(false);
        }
    }

    @Override
    public long getCrawlDelay() {
        return _robotRules.getCrawlDelay();
    }

    @Override
    public boolean getDeferVisits() {
        return _deferVisits;
    }
    
    protected void setDeferVisits(boolean deferVisits) {
        _deferVisits = deferVisits;
    }
    
    protected String getPath(URL url) {
        String path = url.getPath();
        if ((path == null) || (path.equals(""))) {
            path= "/";
        }

        try {
            path = URLDecoder.decode(path, "UTF-8");
        } catch (Exception e) {
            // just ignore it- we can still try to match 
            // path prefixes
        }

        return path;
    }
    
    @Override
    public boolean isAllowed(URL url) {
        String path = getPath(url);
        
        // Always allow robots.txt
        if (path.equalsIgnoreCase("/robots.txt")) {
            return true;
        }
        
        if (_robotRules.allowAll()) {
            return true;
        } else if (_robotRules.allowNone()) {
            return false;
        }
        
        // We always lower-case the path, as anybody who sets up rules that differ only by case
        // is insane, but it's more likely that somebody will accidentally put in rules that don't
        // match their target paths because of case differences.
        return _robotRules.isAllowed(path.toLowerCase());
    }
    
    @Override
    public boolean isAllowed(String url) throws MalformedURLException {
        return isAllowed(new URL(url));
    }
    
    // TODO KKr - catch & report/log issues with the file
    // contains HTML
    // has unknown directives
    // missing user-agent: (got directives other than sitemap before first user-agent)
    // no allow/disallow directive - e.g. Google skips crawl-delay:
    // missing ':' after field name
    // misspelled field names (dissallow, useragent, user-agents)
    // multiple agent names on one line
    // multiple matches for agent name (two sections for '*', for example)
    // conflicting rules (allow & disallow specified for same prefix)
    // invalid path (doesn't start with '/', contains invalid URL characters)
    // uses Google wildcard syntax
    // relies on Google longest path ordering (has longer prefix after shorter prefix)
    // multiple sitemaps
    // multiple crawl delays in same record
    // multiple allows or disallows w/same prefix in same record
    // wildcard in user agent name
    // invalid user agent name (restricted charset from RFP)
    // invalid crawl delay value
    // out-of-bounds crawl delay value
    // invalid sitemap URL
    // mixed domain sitemap URL - must be from same domain as where we got robots.txt?
    // file size exceeds bounds (e.g. 32K)
    // 
    /**
     * Parse the indicated robots.txt file and set up our internal state with the results.
     * 
     * @param robotName - name of robot, for matching against robots.txt
     * @param url - source of robots.txt, for error reporting
     * @param robotContent - raw bytes from robots.txt
     */
    protected void parseRules(String robotName, String url, byte[] robotContent) {
        // If there's nothing there, treat it like we have no restrictions.
        if ((robotContent == null) || (robotContent.length == 0)) {
            LOGGER.trace("Missing/empty robots.txt at " + url);
            createAllOrNone(true);
            return;
        }

        String content;
        try {
            content = new String(robotContent, "us-ascii");
        } catch (UnsupportedEncodingException e) {
            // Should never happen.
            LOGGER.error("Got unsupported encoding exception for us-ascii");
            content = new String(robotContent);
        }

        // Break on anything that might be used as a line ending. Since tokenizer doesn't
        // return empty tokens, a \r\n sequence still works since it looks like an empty
        // string between the \r and \n.
        StringTokenizer lineParser = new StringTokenizer(content, "\n\r\u0085\u2028\u2029");

        RobotRules curRules = new RobotRules();
        boolean matchedRealName = false;
        boolean matchedWildcard = false;
        boolean addingRules = false;
        boolean finishedAgentFields = false;
        
        String targetName = robotName.toLowerCase();
        
        while (lineParser.hasMoreTokens()) {
            String line = lineParser.nextToken();

            // Get rid of HTML markup, in case some brain-dead webmaster has created an HTML
            // page for robots.txt. We could do more sophisticated processing here to better
            // handle bad HTML, but that's a very tiny percentage of all robots.txt files.
            line = line.replaceAll("<[^>]+>","");
            
            // trim out comments and whitespace
            int hashPos = line.indexOf("#");
            if (hashPos >= 0) {
                line = line.substring(0, hashPos);
            }
            line= line.trim().toLowerCase();

            if (line.startsWith(USER_AGENT_FIELD)) {
                if (matchedRealName) {
                    if (finishedAgentFields) {
                        // We're all done.
                        break;
                    } else {
                        // Skip any more of these, once we have a real name match. We're waiting for some
                        // allow/disallow/crawl delay fields.
                        continue;
                    }
                } else if (finishedAgentFields) {
                    // We've got a user agent field, so we haven't yet seen anything that tells us
                    // we're done with this set of agent names.
                    finishedAgentFields = false;
                    addingRules = false;
                }
                
                // TODO KKr - catch case of multiple names, log as non-standard.
                String[] agentNames = line.substring(USER_AGENT_FIELD.length()).trim().split("[ \t,]");
                for (String agentName : agentNames) {
                    if (targetName.contains(agentName)) {
                        matchedRealName = true;
                        addingRules = true;
                        curRules.clearRules();  // In case we previously hit a wildcard rule match
                        break;
                    } else if (agentName.equals("*") && !matchedWildcard) {
                        matchedWildcard = true;
                        addingRules = true;
                    }
                }
            } else if (line.startsWith(DISALLOW_FIELD)) {
                finishedAgentFields = true;
                
                if (!addingRules) {
                    continue;
                }
                
                String path = line.substring(DISALLOW_FIELD.length()).trim();
                
                try {
                    path = URLDecoder.decode(path, "UTF-8");
                } catch (Exception e) {
                    LOGGER.warn("Error parsing robots rules - can't decode path: " + path);
                }

                if (path.length() == 0) {
                    // Disallow: <nothing> => allow all.
                    curRules.clearRules();
                } else {
                    curRules.addRule(path, false);
                }
            } else if (line.startsWith(ALLOW_FIELD)) {
               finishedAgentFields = true;
                
               if (!addingRules) {
                    continue;
                }
                
                String path = line.substring(ALLOW_FIELD.length()).trim();
                
                try {
                    path = URLDecoder.decode(path, "UTF-8");
                } catch (Exception e) {
                    LOGGER.warn("Error parsing robots rules - can't decode path: " + path);
                }

                if (path.length() == 0) {
                    // Allow: <nothing> => allow all.
                    curRules.clearRules();
                } else {
                    curRules.addRule(path, true);
                }
            } else if (line.startsWith(CRAWL_DELAY_FIELD)) {
                finishedAgentFields = true;
                
                if (!addingRules) {
                     continue;
                }
                 
                String delayString = line.substring(CRAWL_DELAY_FIELD.length()).trim();
                if (delayString.length() > 0) {
                    try {
                        long delayValue = Integer.parseInt(delayString) * 1000L; // sec to millisec
                        curRules.setCrawlDelay(delayValue);
                    } catch (Exception e) {
                        LOGGER.info("Error parsing robots rules - can't decode crawl delay", e);
                    }
                }
            } else if (line.startsWith(SITEMAP_FIELD)) {
                String path = line.substring(DISALLOW_FIELD.length()).trim();
               LOGGER.trace("Ignoring sitemap directive in robots.txt: " + path);
            } else if (line.contains(":")) {
            	LOGGER.warn("Unknown directive in robots.txt file: " + line);
                finishedAgentFields = true;
            } else if (line.length() > 0) {
            	LOGGER.warn("Unknown line in robots.txt file: " + line);
                finishedAgentFields = true;
            }
        }

        _robotRules = curRules;
    }
}
