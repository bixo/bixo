package bixo.tools;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class CrawlToolOptions {
    public static final int NO_CRAWL_DURATION = 0;
    public static final int DEFAULT_MAX_THREADS = 10;
    private static final int DEFAULT_NUM_LOOPS = 1;

    private boolean _debugLogging = false;
    private boolean _dryRun = false;

    private String _urlInputFile;
    private String _outputDir;
    private String _agentName;
    
    private int _crawlDuration = NO_CRAWL_DURATION;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _maxUrls = Integer.MAX_VALUE;
    private int _numLoops = DEFAULT_NUM_LOOPS;


    @Option(name = "-d", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        this._debugLogging = debugLogging;
    }

    @Option(name = "-inputfile", usage = "input file with URLs", required = true)
    public void setUrlInputFile(String urlInputFile) {
        _urlInputFile = urlInputFile;
    }

    @Option(name = "-outputdir", usage = "output directory", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    @Option(name = "-agentname", usage = "user agent name", required = true)
    public void setAgentName(String agentName) {
        _agentName = agentName;
    }

    @Option(name = "-maxthreads", usage = "maximum number of fetcher threads to use", required = false)
    public void setMaxThreads(int maxThreads) {
        _maxThreads = maxThreads;
    }

    @Option(name = "-numloops", usage = "number of fetch/update loops", required = false)
    public void setNumLoops(int numLoops) {
        _numLoops = numLoops;
    }

    @Option(name = "-dryrun", usage = "fake fetching", required = false)
    public void setDryRun(boolean dryRun) {
        this._dryRun = dryRun;
    }

    @Option(name = "-duration", usage = "target crawl duration in minutes", required = false)
    public void setCrawlDuration(int crawlDuration) {
        _crawlDuration = crawlDuration;
    }

    @Option(name = "-maxurls", usage = "maximum URLs to process", required = false)
    public void setMaxUrls(int maxUrls) {
        _maxUrls = maxUrls;
    }

    public String getUrlInputFile() {
        return _urlInputFile;
    }

    public String getOutputDir() {
        return _outputDir;
    }

    public String getAgentName() {
        return _agentName;
    }
    
    public int getMaxThreads() {
        return _maxThreads;
    }

    public int getNumLoops() {
        return _numLoops;
    }

    public int getCrawlDuration() {
        return _crawlDuration;
    }

    public int getMaxUrls() {
        return _maxUrls;
    }

    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    public boolean isDryRun() {
        return _dryRun;
    }


    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
