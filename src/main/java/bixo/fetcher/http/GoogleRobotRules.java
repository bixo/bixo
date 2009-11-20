package bixo.fetcher.http;

import java.net.URL;
import java.util.ArrayList;

import javax.servlet.http.HttpServletResponse;

public class GoogleRobotRules extends SimpleRobotRules {
    /**
     * Single rule that maps from a path prefix to an allow flag.
     */
    private class GoogleRobotRule {
        String _prefix;
        boolean _allow;
        boolean _wildcard;
        
        public GoogleRobotRule(String prefix, boolean allow) {
            _prefix = prefix;
            _allow = allow;
            
            // TODO KKr - set up _wildcard if prefix contains special value.
        }
    }
    
    /**
     * Result from parsing a single robots.txt file - which means we
     * get a set of rules, and a crawl-delay.
     */
    @SuppressWarnings("unused")
	private class RobotRules {
        ArrayList<GoogleRobotRule> _googleRules = new ArrayList<GoogleRobotRule>();
        boolean _resync = true;
        
        public RobotRules() {
            super();
            _resync = true;
        }
        
        private void clearRules() {
            // super.clearRules();
            _resync = true;
        }

        private void addRule(String prefix, boolean allow) {
            // super(prefix, allow);
            _resync = true;
        }

        // TODO KKr - make sure paths are sorted from longest to shortest,
        // to implement longest match
        public boolean isAllowed(String path) {
            resync();
            
//            for (RobotRule rule : _rules) {
//                if (path.startsWith(rule._prefix)) {
//                    return rule._allow;
//                }
//            }

            return true;
        }

        private void resync() {
            if (_resync) {
                _resync = false;
                // TODO KKr update our rules to match base rules
            }
        }
    }

    protected GoogleRobotRules() {
        // Hide default constructor
    }
    
    public GoogleRobotRules(String url, int httpStatus) {
        super(url, httpStatus);

        if ((httpStatus == HttpServletResponse.SC_FORBIDDEN) || (httpStatus == HttpServletResponse.SC_UNAUTHORIZED)) {
            // Google treats these types of errors when fetching robots.txt as a sign that
            // the file doesn't exist, so treat it the same as SC_NOT_FOUND.
            createAllOrNone(true);
        }
    }
    
    // TODO KKr - get rid of this version, and add a generic one (robot name, URL) that uses
    // Java URL code to fetch it, as well as a version that takes an output stream (e.g. from
    // HttpClient) and a version that takes a String with the content.
    public GoogleRobotRules(SimpleHttpFetcher fetcher, String url) {
        super(fetcher, url);
    }
    
    public GoogleRobotRules(String robotName, String url, byte[] robotsContent) {
        super(robotName, url, robotsContent);
    }
    
    @Override
    public boolean isAllowed(URL url) {
        String path = getPath(url);
        
        // Always allow robots.txt
        if (path.equalsIgnoreCase("/robots.txt")) {
            return true;
        }
        
        // We always lower-case the path, as anybody who sets up rules that differ only by case
        // is insane, but it's more likely that somebody will accidentally put in rules that don't
        // match their target paths because of case differences.
        
        // TODO KKr - do our wildcard matching.
        return true;
        // return _robotRules.isAllowed(path.toLowerCase());
    }
}
