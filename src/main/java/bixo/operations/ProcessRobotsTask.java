package bixo.operations;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.exceptions.HttpFetchException;
import bixo.exceptions.IOFetchException;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.IRobotRules;
import bixo.fetcher.http.SimpleRobotRules;
import bixo.fetcher.util.ScoreGenerator;
import bixo.utils.DomainNames;
import bixo.utils.GroupingKey;
import cascading.tuple.TupleEntryCollector;

public class ProcessRobotsTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ProcessRobotsTask.class);
    
    public static class DomainInfo {
        
        // Pay no attention to this cheesy hack - special case handling of certain domains
        // so we can test without trying to resolve domain names.
        private static final Pattern TESTING_DOMAIN_PATTERN = Pattern.compile("bixo-test-domain-\\d+\\.com");
        
        private String _protocolAndDomain;
        private String _domain;
        private String _hostAddress;
        
        public DomainInfo(String protocolAndDomain) throws UnknownHostException, MalformedURLException {
            _protocolAndDomain = protocolAndDomain;
            _domain = new URL(protocolAndDomain).getHost();
            
            if (TESTING_DOMAIN_PATTERN.matcher(_domain).matches()) {
                _hostAddress = _domain;
            } else {
                _hostAddress = InetAddress.getByName(_domain).getHostAddress();
            }
        }

        public String getProtocolAndDomain() {
            return _protocolAndDomain;
        }

        public String getDomain() {
            return _domain;
        }

        public String getHostAddress() {
            return _hostAddress;
        }
    }
    
    private DomainInfo _domainInfo;
    private ScoreGenerator _scorer;
    private Queue<GroupedUrlDatum> _urls;
    private IHttpFetcher _fetcher;
    private TupleEntryCollector _collector;
    
    
    public ProcessRobotsTask(DomainInfo domainInfo, ScoreGenerator scorer, Queue<GroupedUrlDatum> urls, IHttpFetcher fetcher, TupleEntryCollector collector) {
        _domainInfo = domainInfo;
        _scorer = scorer;
        _urls = urls;
        _fetcher = fetcher;
        _collector = collector;
    }

    @Override
    public void run() {
        String domain = _domainInfo.getDomain();
        String pld = DomainNames.getPLD(domain);
        if (!_scorer.isGoodDomain(domain, pld)) {
            // TODO KKr - don't lose URLs.
            LOGGER.debug("Skipping URLs from not-good domain: " + domain);
            return;
        }
        
        String robotsUrl = "";
        IRobotRules robotRules = null;

        try {
            robotsUrl = new URL(_domainInfo.getProtocolAndDomain() + "/robots.txt").toExternalForm();
            byte[] robotsContent = _fetcher.get(robotsUrl);
            robotRules = new SimpleRobotRules(_fetcher.getUserAgent().getAgentName(), robotsUrl, robotsContent);
        } catch (HttpFetchException e) {
            robotRules = new SimpleRobotRules(robotsUrl, e.getHttpStatus());
        } catch (IOFetchException e) {
            // Couldn't load robots.txt for some reason (e.g. ConnectTimeoutException), so
            // treat it like a server internal error case.
            // We don't cache it, so that next time we have to reload it
            robotRules = new SimpleRobotRules(robotsUrl, HttpStatus.SC_INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.warn("Unexpected exception handling robots.txt: " + robotsUrl, e);
            // We don't cache it, so that next time we have to reload it
            robotRules = new SimpleRobotRules(robotsUrl, HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }

        String key;
        if (robotRules.getDeferVisits()) {
            // We don't want to toss these URLs just because the server is acting up, so use the special
            // key so that the FetchBuffer will emit status, and we can try again later.
            key = GroupingKey.DEFERRED_GROUPING_KEY;
        } else {
            key = GroupingKey.makeGroupingKey(_domainInfo.getHostAddress(), robotRules.getCrawlDelay());
        }

        // Use the same key for every URL from this domain
        GroupedUrlDatum datum;
        while ((datum = _urls.poll()) != null) {
            double score = _scorer.generateScore(domain, pld, datum.getUrl());
            ScoredUrlDatum scoreUrl = new ScoredUrlDatum(datum.getUrl(), 0, 0, UrlStatus.UNFETCHED, key, score, datum.getMetaDataMap());
            synchronized (_collector) {
                // collectors aren't thread safe
                _collector.add(scoreUrl.toTuple());
            }
        }
    }
}
