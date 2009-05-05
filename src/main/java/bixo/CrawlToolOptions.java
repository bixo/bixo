package bixo;

import org.kohsuke.args4j.Option;

public class CrawlToolOptions {
	private String _urlInputFile;
	private String _outputDir;
	private int _maxThreads;
	private int _numLoops;
	
	public CrawlToolOptions() {
	    // Set default options
		_maxThreads = 10;
		_numLoops = 1;
	}
	
	@Option(name = "-urls", usage = "input file with URLs", required = true)
	public void setUrlInputFile(String urlInputFile) {
		_urlInputFile = urlInputFile;
	}
	
	@Option(name = "-outdir", usage = "output directory", required = true)
	public void setOutputDir(String outputDir) {
		_outputDir = outputDir;
	}

	@Option(name = "-maxthreads", usage = "maximum number of fetcher threads to use", required = false)
	public void setMaxThreads(int maxThreads) {
		_maxThreads = maxThreads;
	}

	@Option(name = "-numloops", usage = "number of fetch/update loops", required = false)
	public void setNumLoops(int numLoops) {
	    _numLoops = numLoops;
	}
	
	public String getUrlInputFile() {
		return _urlInputFile;
	}
	
	public String getOutputDir() {
		return _outputDir;
	}
	
    public int getMaxThreads() {
        return _maxThreads;
    }
    
    public int getNumLoops() {
        return _numLoops;
    }
    
	
}
