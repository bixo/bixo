/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.RobotUtils;
import crawlercommons.robots.SimpleRobotRulesParser;
import bixo.fetcher.BaseFetcher;
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
        // Uncomment this to see the wire log for HttpClient
        // System.setProperty("bixo.http.level", "DEBUG");
        
        BaseFetcher fetcher = RobotUtils.createFetcher(ConfigUtils.BIXO_TOOL_AGENT, 1);
        
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

            	URL robotsUrl = new URL(url);
            	if (!robotsUrl.getPath().toLowerCase().endsWith("/robots.txt")) {
            	    robotsUrl = new URL(robotsUrl, "/robots.txt");
            	}
            	
            	System.out.println("Processing " + robotsUrl.toExternalForm());
            	BaseRobotRules rules = RobotUtils.getRobotRules(fetcher, new SimpleRobotRulesParser(), robotsUrl);
                System.out.println(String.format("Deferred visits = %s, allow all = %s, allow none = %s, top-level allowed = %s",
                                rules.isDeferVisits(),
                                rules.isAllowAll(),
                                rules.isAllowNone(),
                                rules.isAllowed(UrlUtils.makeProtocolAndDomain(url))));
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
