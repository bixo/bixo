package bixo.operations;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Queue;

import org.apache.log4j.Logger;

import bixo.cascading.BixoFlowProcess;
import bixo.datum.GroupedUrlDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.IRobotRules;
import bixo.fetcher.http.SimpleRobotRules;
import bixo.fetcher.util.ScoreGenerator;
import bixo.hadoop.FetchCounters;
import bixo.utils.DomainInfo;
import bixo.utils.DomainNames;
import bixo.utils.GroupingKey;
import cascading.tuple.TupleEntryCollector;

public class ProcessRobotsTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ProcessRobotsTask.class);

    
    private String _protocolAndDomain;
    private ScoreGenerator _scorer;
    private Queue<GroupedUrlDatum> _urls;
    private IHttpFetcher _fetcher;
    private TupleEntryCollector _collector;
    private BixoFlowProcess _flowProcess;

    public ProcessRobotsTask(String protocolAndDomain, ScoreGenerator scorer, Queue<GroupedUrlDatum> urls, IHttpFetcher fetcher, TupleEntryCollector collector, BixoFlowProcess flowProcess) {
        _protocolAndDomain = protocolAndDomain;
        _scorer = scorer;
        _urls = urls;
        _fetcher = fetcher;
        _collector = collector;
        _flowProcess = flowProcess;
    }

    /**
     * Clear out the queue by outputting all entries with <groupingKey>.
     * 
     * We do this to empty the queue when there's some kind of error.
     * 
     * @param urls Queue of URLs to empty out
     * @param groupingKey grouping key to use for all entries.
     * @param outputCollector
     */
    public static void emptyQueue(Queue<GroupedUrlDatum> urls, String groupingKey, TupleEntryCollector collector) {
        GroupedUrlDatum datum;
        while ((datum = urls.poll()) != null) {
            ScoredUrlDatum scoreUrl = new ScoredUrlDatum(datum.getUrl(), 0, 0, UrlStatus.UNFETCHED, groupingKey, 1.0, datum.getMetaDataMap());
            synchronized (collector) {
                collector.add(scoreUrl.toTuple());
            }
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     * 
     * Get robots.txt for the domain, and use it to generate a new grouping key
     * for all of the URLs that provides the count & crawl delay (or deferred/blocked)
     * values that we need.
     */
    @Override
    public void run() {
        _flowProcess.increment(FetchCounters.DOMAINS_PROCESSING, 1);

        try {
            DomainInfo domainInfo = new DomainInfo(_protocolAndDomain);
            if (!domainInfo.isValidHostAddress()) {
                throw new UnknownHostException(_protocolAndDomain);
            }
            
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Resolved %s to %s", _protocolAndDomain, domainInfo.getHostAddress()));
            }
            
            String domain = domainInfo.getDomain();
            String pld = DomainNames.getPLD(domain);
            if (!_scorer.isGoodDomain(domain, pld)) {
                _flowProcess.increment(FetchCounters.DOMAINS_SKIPPED, 1);
                LOGGER.debug("Skipping URLs from not-good domain: " + domain);
                emptyQueue(_urls, GroupingKey.SKIPPED_GROUPING_KEY, _collector);
            } else {
                String robotsUrl = new URL(domainInfo.getProtocolAndDomain() + "/robots.txt").toExternalForm();
                IRobotRules robotRules = new SimpleRobotRules(_fetcher, robotsUrl);

                String key;
                if (robotRules.getDeferVisits()) {
                    // We don't want to toss these URLs just because the server is
                    // acting up, so use the special
                    // key so that the FetchBuffer will emit status, and we can try
                    // again later.
                    LOGGER.debug("Deferring visits to URLs from " + domainInfo.getDomain());
                    key = GroupingKey.DEFERRED_GROUPING_KEY;
                    _flowProcess.increment(FetchCounters.DOMAINS_DEFERRED, 1);
                } else {
                    // TODO KKr - stop passing count in, since that's not workable for keys (multiple domains
                    // map to the same IP address, so count-<ip> throws off downstream processing.
                    key = GroupingKey.makeGroupingKey(0, domainInfo.getHostAddress(), robotRules.getCrawlDelay());
                    _flowProcess.increment(FetchCounters.DOMAINS_FINISHED, 1);
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
        } catch (UnknownHostException e) {
            LOGGER.debug("Unknown host", e);
            _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
            emptyQueue(_urls, GroupingKey.UNKNOWN_HOST_GROUPING_KEY, _collector);
        } catch (MalformedURLException e) {
            LOGGER.debug("Invalid URL: " + _protocolAndDomain);
            _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
            emptyQueue(_urls, GroupingKey.INVALID_URL_GROUPING_KEY, _collector);
        } catch (Exception e) {
            LOGGER.warn("Exception processing " + _protocolAndDomain, e);
            _flowProcess.increment(FetchCounters.DOMAINS_REJECTED, 1);
            emptyQueue(_urls, GroupingKey.INVALID_URL_GROUPING_KEY, _collector);
        } finally {
            _flowProcess.decrement(FetchCounters.DOMAINS_PROCESSING, 1);
            _flowProcess.decrement(FetchCounters.DOMAINS_REMAINING, 1);
        }
    }
}
