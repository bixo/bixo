package bixo.robots;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import bixo.robots.SimpleRobotRules.RobotRulesMode;


@SuppressWarnings("serial")
public class SimpleRobotRulesParser extends RobotRulesParser {
    private static final Logger LOGGER = Logger.getLogger(SimpleRobotRulesParser.class);
    
    private static final Pattern SIMPLE_HTML_PATTERN = Pattern.compile("(?is)<(html|head|body)\\s*>");
    private static final Pattern USER_AGENT_PATTERN = Pattern.compile("(?i)user-agent:");

    // These must be lower-case, for matching.
    private static final String USER_AGENT_FIELD = "user-agent:";
    private static final String DISALLOW_FIELD = "disallow:";
    private static final String ALLOW_FIELD = "allow:";
    private static final String CRAWL_DELAY_FIELD = "crawl-delay:";
    private static final String SITEMAP_FIELD = "sitemap:";
    private static final String HOST_FIELD = "host:";
    private static final String NO_INDEX_FIELD = "noindex:";
    private static final String ACAP_FIELD = "acap-";

    // Max # of warnings during parse of any one robots.txt file.
    private static final int MAX_WARNINGS = 5;
    
    // Max value for crawl delay we'll use from robots.txt file. If the value is greater
    // than this, we'll skip all pages.
    private static final long MAX_CRAWL_DELAY = 200000;

    private int _numWarnings;
    
    @Override
    public RobotRules failedFetch(int httpStatusCode) {
        SimpleRobotRules result;
        
        if ((httpStatusCode >= 200) && (httpStatusCode < 300)) {
            throw new IllegalStateException("Can't use status code constructor with 2xx response");
        } else if ((httpStatusCode >= 300) && (httpStatusCode < 400)) {
            // Should only happen if we're getting endless redirects (more than our follow limit), so
            // treat it as a temporary failure.
            result = new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);
            result.setDeferVisits(true);
        } else if ((httpStatusCode >= 400) && (httpStatusCode < 500)) {
            // Some sites return 410 (gone) instead of 404 (not found), so treat as the same.
            // Actually treat all (including forbidden) as "no robots.txt", as that's what Google
            // and other search engines do.
            result = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
        } else {
            // Treat all other status codes as a temporary failure.
            result = new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);
            result.setDeferVisits(true);
        }
        
        return result;
    }

    @Override
    public RobotRules parseContent(String url, byte[] content, String contentType, String robotName) {
        _numWarnings = 0;
        
        // If there's nothing there, treat it like we have no restrictions.
        if ((content == null) || (content.length == 0)) {
            return new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
        }

        int bytesLen = content.length;
        int offset = 0;
        String encoding = "us-ascii";
        
        // Check for a UTF-8 BOM at the beginning (EF BB BF)
        if ((bytesLen >= 3) && (content[0] == (byte)0xEF) && (content[1] == (byte)0xBB) && (content[2] == (byte)0xBF)) {
            offset = 3;
            bytesLen -= 3;
            encoding = "UTF-8";
        }
        
        String contentAsStr;
        try {
            contentAsStr = new String(content, offset, bytesLen, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Got unsupported encoding exception for " + encoding);
        }

        // Decide if we need to do special HTML processing.
        boolean isHtmlType = ((contentType != null) && contentType.toLowerCase().startsWith("text/html"));
        
        // If it looks like it contains HTML, but doesn't have a user agent field, then
        // assume somebody messed up and returned back to us a random HTML page instead
        // of a robots.txt file.
        boolean hasHTML = false;
        if (isHtmlType || SIMPLE_HTML_PATTERN.matcher(contentAsStr).find()) {
            if (!USER_AGENT_PATTERN.matcher(contentAsStr).find()) {
                LOGGER.trace("Found non-robots.txt HTML file: " + url);
                return new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
            } else {
                // We'll try to strip out HTML tags below.
                if (isHtmlType) {
                    LOGGER.debug("HTML content type returned for robots.txt file: " + url);
                } else {
                    LOGGER.debug("Found HTML in robots.txt file: " + url);
                }
                
                hasHTML = true;
            }
        }
        
        // Break on anything that might be used as a line ending. Since tokenizer doesn't
        // return empty tokens, a \r\n sequence still works since it looks like an empty
        // string between the \r and \n.
        StringTokenizer lineParser = new StringTokenizer(contentAsStr, "\n\r\u0085\u2028\u2029");

        SimpleRobotRules curRules = new SimpleRobotRules();
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
            if (hasHTML) {
                line = line.replaceAll("<[^>]+>","");
            }
            
            // trim out comments and whitespace
            int hashPos = line.indexOf("#");
            if (hashPos >= 0) {
                line = line.substring(0, hashPos);
            }
            line = line.trim().toLowerCase();

            // TODO KKr - use regex versus line.startsWith, so we can handle things
            // like common typos, missing ':'
            if (line.length() == 0) {
                // Skip blank lines.
            } else if (line.startsWith(USER_AGENT_FIELD)) {
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
                    
                    if (path.length() == 0) {
                        // Disallow: <nothing> => allow all.
                        curRules.clearRules();
                    } else {
                        curRules.addRule(path, false);
                    }
                } catch (Exception e) {
                    reportWarning("Error parsing robots rules - can't decode path: " + path, url);
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
                    reportWarning("Error parsing robots rules - can't decode path: " + path, url);
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
                        // Some sites use values like 0.5 for the delay.
                        if (delayString.indexOf('.') != -1) {
                            double delayValue = Double.parseDouble(delayString) * 1000.0;
                            curRules.setCrawlDelay(Math.round(delayValue));
                        } else {
                            long delayValue = Integer.parseInt(delayString) * 1000L; // sec to millisec
                            curRules.setCrawlDelay(delayValue);
                        }
                    } catch (Exception e) {
                        reportWarning("Error parsing robots rules - can't decode crawl delay: " + delayString, url);
                    }
                }
            }

            // TODO KKr - which of these should be setting finishedAgentFields to true?
            
            else if (line.startsWith(SITEMAP_FIELD)) {
                // Ignore for now
            } else if (line.startsWith(HOST_FIELD)) {
                // Russian-specific directive for mirror site?
                // Used by the zerkalschik robot?
                // See http://wataro.ur/en/web/robot.html
            } else if (line.startsWith(NO_INDEX_FIELD)) {
                // Ignore Google extension
            } else if (line.startsWith(ACAP_FIELD)) {
                // Ignore ACAP extensions
            } else if (line.contains(":")) {
                reportWarning("Unknown directive in robots.txt file: " + line, url);
                finishedAgentFields = true;
            } else if (line.length() > 0) {
                reportWarning(String.format("Unknown line in robots.txt file (size %d): %s", content.length, line), url);
                finishedAgentFields = true;
            }
        }

        if (curRules.getCrawlDelay() > MAX_CRAWL_DELAY) {
            // Some evil sites use a value like 3600 (seconds) for the crawl delay, which would
            // cause lots of problems for us.
            reportWarning("Crawl delay exceeds max value - so disallowing all URLs", url);
            return new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);
        } else {
            return curRules;
        }
    }

    private void reportWarning(String msg, String url) {
        _numWarnings += 1;
        
        if (_numWarnings == 1) {
            LOGGER.warn("Problem processing robots.txt for " + url);
        }
        
        if (_numWarnings < MAX_WARNINGS) {
            LOGGER.warn("\t" + msg);
        }
    }
    
    // For testing
    public int getNumWarnings() {
        return _numWarnings;
    }
    
}
