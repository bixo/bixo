package bixo.fetcher.http;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import bixo.datum.FetchStatusCode;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;

public class SimpleRobotRules implements IRobotRules {
    private static final Logger LOGGER = Logger.getLogger(SimpleRobotRules.class);
    
    private static final int NO_PRECEDENCE = Integer.MAX_VALUE;

    private boolean _allowAll;
    private boolean _allowNone;
    private RobotRules _robotRules;
    
    /**
     * Single rule that maps from a path prefix to an allow flag.
     *
     */
    private class RobotRule {
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
     * TODO KKr - add support for keep-alive
     *
     */
    private class RobotRules {
        ArrayList<RobotRule> _tmpEntries = new ArrayList<RobotRule>();
        RobotRule[] _entries = null;
        private int _crawlDelay = NO_CRAWL_DELAY;

        private void clearPrefixes() {
            if (_tmpEntries == null) {
                _tmpEntries= new ArrayList<RobotRule>();
                _entries= null;
            } else {
                _tmpEntries.clear();
            }
        }

        private void addPrefix(String prefix, boolean allow) {
            if (_tmpEntries == null) {
                _tmpEntries= new ArrayList<RobotRule>();
                if (_entries != null) {
                    for (int i= 0; i < _entries.length; i++) 
                        _tmpEntries.add(_entries[i]);
                }
                _entries= null;
            }

            _tmpEntries.add(new RobotRule(prefix, allow));
        }

        public int getCrawlDelay() {
            return _crawlDelay;
        }

        public void setCrawlDelay(int crawlDelay) {
            _crawlDelay = crawlDelay;
        }

        public boolean isAllowed(String path) {
            if (_entries == null) {
                _entries = _tmpEntries.toArray(new RobotRule[_tmpEntries.size()]);
                _tmpEntries= null;
            }

            int pos= 0;
            int end = _entries.length;
            while (pos < end) {
                if (path.startsWith(_entries[pos]._prefix)) {
                    return _entries[pos]._allow;
                }
                
                pos++;
            }

            return true;
        }
    }
    
    public SimpleRobotRules(String robotName, HttpClientFetcher fetcher, String url) {
        try {
            URL realUrl = new URL(url);
            String urlToFetch = new URL(realUrl, "/robots.txt").toExternalForm();
            
            ScoredUrlDatum scoredUrl = new ScoredUrlDatum(urlToFetch, 0, 0, FetchStatusCode.UNFETCHED, urlToFetch, null, 1.0, null);
            FetchedDatum result = fetcher.get(scoredUrl);
            
            if (result.getStatusCode() == FetchStatusCode.FETCHED) {
                parseRules(robotName, urlToFetch, result.getContent().getBytes());
            } else {
                // TODO KKr - treat forbidden as ALLOW_NONE
                createAllOrNone(true);
            }
        } catch (MalformedURLException e) {
            LOGGER.warn("Invalid URL: " + url);
            createAllOrNone(false);
        }
        
    }
    
    public SimpleRobotRules(String robotName, byte[] robotsContent) {
        parseRules(robotName, "url", robotsContent);
    }
    
    private void createAllOrNone(boolean allowAll) {
        _robotRules = null;
        _allowAll = allowAll;
        _allowNone = !allowAll;
    }
    
    @Override
    public int getCrawlDelay() {
        if (_robotRules == null) {
            return NO_CRAWL_DELAY;
        } else {
            return _robotRules.getCrawlDelay();
        }
    }

    @Override
    public boolean isAllowed(String url) {
        if (_allowAll) {
            return true;
        } else if (_allowNone) {
            return false;
        }
        
        String path;
        
        try {
            URL realUrl = new URL(url);
            path = realUrl.getPath();
            if ((path == null) || (path.equals(""))) {
                path= "/";
            }
        } catch (MalformedURLException e) {
            return false;
        }
        
        try {
            path = URLDecoder.decode(path, "UTF-8");
        } catch (Exception e) {
          // just ignore it- we can still try to match 
          // path prefixes
        }
        
        return _robotRules.isAllowed(path);
    }
    
    /**
     * Parse the indicated robots.txt file and set up our internal state with the results.
     * 
     * @param robotName - name of robot, for matching against robots.txt
     * @param url - source of robots.txt, for error reporting
     * @param robotContent - raw bytes from robots.txt
     */
    private void parseRules(String robotName, String url, byte[] robotContent) {
        // If there's nothing there, treat it like we have no restrictions.
        if ((robotContent == null) || (robotContent.length == 0)) {
            LOGGER.trace("Missing/empty robots.txt at " + url);
            createAllOrNone(true);
            return;
        }

        HashMap<String, Integer> robotNames = new HashMap<String, Integer>();
        robotNames.put(robotName, new Integer(0));
        robotNames.put("*", new Integer(1));

        String content;
        try {
            content= new String(robotContent, "us-ascii");
        } catch (UnsupportedEncodingException e) {
            // Should never happen.
            LOGGER.error("Got unsupported encoding exception for us-ascii");
            content = new String(robotContent);
        }
        
        StringTokenizer lineParser= new StringTokenizer(content, "\n\r");

        RobotRules bestRulesSoFar = null;
        int bestPrecedenceSoFar = NO_PRECEDENCE;

        RobotRules currentRules = new RobotRules();
        int currentPrecedence= NO_PRECEDENCE;

        boolean addRules = false;    // in stanza for our robot
        boolean doneAgents = false;  // detect multiple agent lines

        while (lineParser.hasMoreTokens()) {
            String line = lineParser.nextToken();

            // trim out comments and whitespace
            int hashPos = line.indexOf("#");
            if (hashPos >= 0) {
                line = line.substring(0, hashPos);
            }
            line= line.trim();

            if ((line.length() >= 11)  && (line.substring(0, 11).equalsIgnoreCase("User-agent:"))) {
                if (doneAgents) {
                    if (currentPrecedence < bestPrecedenceSoFar) {
                        bestPrecedenceSoFar= currentPrecedence;
                        bestRulesSoFar= currentRules;
                        currentPrecedence= NO_PRECEDENCE;
                        currentRules= new RobotRules();
                    }
                    
                    addRules= false;
                }
                
                doneAgents= false;

                String agentNames = line.substring(line.indexOf(":") + 1);
                agentNames = agentNames.trim();
                StringTokenizer agentTokenizer = new StringTokenizer(agentNames);

                while (agentTokenizer.hasMoreTokens()) {
                    // for each agent listed, see if it's us:
                    String agentName = agentTokenizer.nextToken().toLowerCase();

                    Integer precedenceInt = robotNames.get(agentName);

                    if (precedenceInt != null) {
                        int precedence= precedenceInt.intValue();
                        if ((precedence < currentPrecedence) && (precedence < bestPrecedenceSoFar)) {
                            currentPrecedence= precedence;
                        }
                    }
                }

                if (currentPrecedence < bestPrecedenceSoFar) {
                    addRules= true;
                }
            } else if ((line.length() >= 9) && (line.substring(0, 9).equalsIgnoreCase("Disallow:")) ) {
                doneAgents = true;
                String path = line.substring(line.indexOf(":") + 1);
                path= path.trim();
                
                try {
                    path = URLDecoder.decode(path, "UTF-8");
                } catch (Exception e) {
                    LOGGER.warn("Error parsing robots rules- can't decode path: " + path);
                }

                if (path.length() == 0) { // "empty rule"
                    if (addRules) {
                        currentRules.clearPrefixes();
                    }
                } else {  // rule with path
                    if (addRules) {
                        currentRules.addPrefix(path, false);
                    }
                }

            } else if ((line.length() >= 6) && (line.substring(0, 6).equalsIgnoreCase("Allow:")) ) {
                doneAgents= true;
                String path= line.substring(line.indexOf(":") + 1);
                path= path.trim();

                if (path.length() == 0) { 
                    // "empty rule"- treat same as empty disallow
                    if (addRules)
                        currentRules.clearPrefixes();
                } else {  // rule with path
                    if (addRules)
                        currentRules.addPrefix(path, true);
                }
            } else if ((line.length() >= 12) && (line.substring(0, 12).equalsIgnoreCase("Crawl-Delay:"))) {
                doneAgents = true;
                if (addRules) {
                    int crawlDelay = NO_CRAWL_DELAY;
                    String delay = line.substring("Crawl-Delay:".length(), line.length()).trim();
                    if (delay.length() > 0) {
                        try {
                            crawlDelay = Integer.parseInt(delay) * 1000; // sec to millisec
                        } catch (Exception e) {
                            LOGGER.info("can not parse Crawl-Delay: " + e.toString());
                        }
                        
                        currentRules.setCrawlDelay(crawlDelay);
                    }
                }
            }
        }

        if (currentPrecedence < bestPrecedenceSoFar) {
            bestPrecedenceSoFar = currentPrecedence;
            bestRulesSoFar = currentRules;
        }

        if (bestPrecedenceSoFar == NO_PRECEDENCE) {
            createAllOrNone(true);
        } else {
            _robotRules = bestRulesSoFar;
        }
    }
}
