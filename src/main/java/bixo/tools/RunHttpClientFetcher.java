package bixo.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import bixo.config.FetcherPolicy;
import bixo.datum.FetchedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.http.SimpleHttpFetcher;

public class RunHttpClientFetcher {

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

    /**
     * @param args - URL to fetch
     */
    public static void main(String[] args) {
    	// Use standard Firefox agent name, as some sites won't work w/non-standard names.
        String userAgent = "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.8) Gecko/2009032608 Firefox/3.0.8";
        
        // Just to be really robust, allow a huge number of redirects and retries.
        FetcherPolicy policy = new FetcherPolicy();
        policy.setMaxRedirects(100);
        SimpleHttpFetcher fetcher = new SimpleHttpFetcher(1, policy, userAgent);
        fetcher.setMaxRetryCount(100);
        
        boolean interactive = args.length == 0;
        int index = 0;
        
        while (interactive || (index < args.length)) {
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
        		System.out.println("Result = " + result.toString());
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
