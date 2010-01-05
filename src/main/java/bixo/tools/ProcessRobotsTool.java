package bixo.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import bixo.fetcher.http.IHttpFetcher;
import bixo.fetcher.http.SimpleRobotRules;
import bixo.operations.FilterAndScoreByUrlAndRobots;
import bixo.utils.ConfigUtils;
import bixo.utils.UrlUtils;

public class ProcessRobotsTool {

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
        System.setProperty("bixo.root.level", "TRACE");

        IHttpFetcher fetcher = FilterAndScoreByUrlAndRobots.createFetcher(ConfigUtils.BIXO_TOOL_AGENT, 1);
        
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

            	System.out.println("Processing " + url);
            	SimpleRobotRules rules = new SimpleRobotRules(fetcher, url);
                System.out.println(String.format("Deferred visits = %s, top-level allowed = %s, is HTML = %s",
                                rules.getDeferVisits(), rules.isAllowed(UrlUtils.makeProtocolAndDomain(url)), rules.isHtmlType()));
                System.out.println();
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
