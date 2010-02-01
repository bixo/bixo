package bixo.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.SimpleHttpFetcher;
import bixo.parser.SimpleParser;

public class FetchAndParseTool {

	@SuppressWarnings("serial")
	private static class FirefoxUserAgent extends UserAgent {
		public FirefoxUserAgent() {
			super("Firefox", "", "");
		}
		
		@Override
		public String getUserAgentString() {
	    	// Use standard Firefox agent name, as some sites won't work w/non-standard names.
			return "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.8) Gecko/2009032608 Firefox/3.0.8";
		}
	}
	
    private static String readInputLine() throws IOException {
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(isr);
        
        try {
            return br.readLine();
        } finally {
            // TODO KKr - will this actually close System.in?
            // Should I reuse this buffered reader? Check out password masking code.
            // br.close();
        }
    }

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    /**
     * @param args - URL to fetch
     */
    public static void main(String[] args) {
        FetchAndParseToolOptions options = new FetchAndParseToolOptions();
        CmdLineParser cmdParser = new CmdLineParser(options);
        
        try {
            cmdParser.parseArgument(args);
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(cmdParser);
        }

        // Just to be really robust, allow a huge number of redirects and retries.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxRedirects(options.getMaxRedirects());
        policy.setMaxContentSize(options.getMaxSize());
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, new FirefoxUserAgent());
        fetcher.setMaxRetryCount(options.getMaxRetries());
        
        String urls[] = options.getUrls() == null ? null : options.getUrls().split(",");
        boolean interactive = (urls == null);
        int index = 0;
        
        while (interactive || (index < urls.length)) {
        	String url;
        	
        	try {
            	if (interactive) {
            		System.out.print("URL to fetch: ");
            		url = readInputLine();
            		if (url.length() == 0) {
            			System.exit(0);
            		}
            	} else {
            		url = args[index++];
            	}

            	System.out.println("Fetching " + url);
        		FetchedDatum result = fetcher.get(new ScoredUrlDatum(url));
        		System.out.println(String.format("Fetched %s: headers = %s", result.getBaseUrl(), result.getHeaders()));
        		System.out.flush();
        		
        		// System.out.println("Result = " + result.toString());
        		SimpleParser parser = new SimpleParser();
        		ParsedDatum parsed = parser.parse(result);
        		System.out.println(String.format("Parsed %s: lang = %s, size = %d", parsed.getUrl(),
        		                parsed.getLanguage(), parsed.getParsedText().length()));
        	} catch (Exception e) {
        		e.printStackTrace(System.out);
                
        		if (interactive) {
        		    System.out.println();
        		    System.out.flush();
        		} else {
        			System.exit(-1);
        		}
        	}
        }
    }

}
