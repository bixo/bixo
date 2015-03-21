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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import bixo.config.FetcherPolicy;
import bixo.config.ParserPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.ParsedDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.parser.BoilerpipeContentExtractor;
import bixo.parser.NullLinkExtractor;
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

    private static final int MAX_PARSE_DURATION = 180 * 1000;
	
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
        
        // Give a long timeout for parsing
        ParserPolicy parserPolicy = new ParserPolicy(MAX_PARSE_DURATION);
        SimpleParser parser = new SimpleParser(parserPolicy);

        SimpleParser rawParser = new SimpleParser(parserPolicy, true);
        
        // Create Boilperpipe content extractor
        SimpleParser bpParser = new SimpleParser(new BoilerpipeContentExtractor(), new NullLinkExtractor(), parserPolicy);
        
        if (options.isTraceLogging()) {
            Logger.getRootLogger().setLevel(Level.TRACE);
            System.setProperty("bixo.root.level", "TRACE");
        }
        
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
        		System.out.println(String.format("Fetched %s: headers = %s", result.getUrl(), result.getHeaders()));
        		System.out.flush();
        		
        		// System.out.println("Result = " + result.toString());
        		ParsedDatum parsed = parser.parse(result);
        		System.out.println(String.format("Parsed %s: lang = %s, size = %d", parsed.getUrl(),
        		                parsed.getLanguage(), parsed.getParsedText().length()));
        		
        		ParsedDatum bpParsed = bpParser.parse(result);
        		ParsedDatum rawParsed = rawParser.parse(result);
        		
        		if (interactive) {
        		    while (true) {
        		        System.out.print("Next action - (d)ump regular, dump (b)oilerpipe, dump (r)aw, (e)xit: ");
        		        String action = readInputLine();
        		        if (action.startsWith("e") || (action.length() == 0)) {
        		            break;
                        } else if (action.startsWith("d")) {
                            System.out.println("=====================================================================");
                            System.out.println(parsed.getParsedText());
                            System.out.println("=====================================================================");
                        } else if (action.startsWith("b")) {
                            System.out.println("=====================================================================");
                            System.out.println(bpParsed.getParsedText());
                            System.out.println("=====================================================================");
                        } else if (action.startsWith("r")) {
                            System.out.println("=====================================================================");
                            System.out.println(rawParsed.getParsedText());
                            System.out.println("=====================================================================");
        		        } else {
        		            System.out.println("Unknown command - " + action);
        		        }
        		    }
        		}
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
