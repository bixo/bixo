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
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractDmozAdultDomains {

    public static void main(String[] args) {

        try {

            // <ExternalPage about="http://www.liquidgeneration.com/">
            //   <d:Title>Liquid Generation</d:Title>
            //   <d:Description>Entertainment of a variety of genres, including games, cartoons, music and celebrity spoofs. [Flash required]</d:Description>
            //   <topic>Top/Adult/Arts</topic>
            // </ExternalPage>

            Pattern externalPageStartPattern = Pattern.compile("<ExternalPage\\s+about\\s*=\\s*\\\"(http[^\\\"]+)\\\"\\s*>.*");
            Pattern externalPageEndPattern = Pattern.compile("</ExternalPage>");
            Pattern topicPattern = Pattern.compile("\\s+<topic>(.*)</topic>");
            
            Matcher epsMatcher = externalPageStartPattern.matcher("");
            Matcher epeMatcher = externalPageEndPattern.matcher("");
            Matcher topicMatcher = topicPattern.matcher("");
            
            // Count of # of adult URLs found for domain, minus number of non-adult URLs
            // So if > 0 then assume adult, otherwise not
            HashMap<String, Integer> domains = new HashMap<String, Integer>(40000);

            String inputFile = args[0];
            PrintStream outputStream;
            if (args.length == 2) {
                outputStream = new PrintStream(new File(args[1]));
            } else {
                outputStream = System.out;
            }

            InputStreamReader isr = new InputStreamReader(new FileInputStream(inputFile), "UTF-8");
            BufferedReader reader = new BufferedReader(isr);

            int numLines = 0;
            int numAdultUrls = 0;

            boolean inExternalPage = false;

            String curLine;
            String urlString = "";
            
            while ((curLine = reader.readLine()) != null) {
                numLines += 1;
                if ((numLines % 100000) == 0) {
                    System.out.println(String.format(" %d/%d", numLines, numAdultUrls));
                } else if ((numLines % 1000) == 0) {
                    System.out.print('.');
                }

                if (inExternalPage) {
                    if (epeMatcher.reset(curLine).matches()) {
                        inExternalPage = false;
                    } else if (topicMatcher.reset(curLine).matches()) {
                        try {
                            URL url = new URL(URLDecoder.decode(urlString, "UTF-8"));
                            String hostname = url.getHost();
                            boolean isAdult = topicMatcher.group(1).startsWith("Top/Adult");
                            
                            if (isAdult) {
                                numAdultUrls += 1;
                                
                                Integer adultCount = domains.get(hostname);
                                if (adultCount == null) {
                                    adultCount = new Integer(1);
                                } else {
                                    adultCount += 1;
                                }
                                
                                domains.put(hostname, adultCount);
                            } else {
                                // Since all of the Top/Adult entries come first, we only care
                                // about decrementing counts for potentially adult domains.
                                Integer adultCount = domains.get(hostname);
                                if (adultCount != null) {
                                    adultCount -= 1;
                                    if (adultCount <= 0) {
                                        domains.remove(hostname);
                                    } else {
                                        domains.put(hostname, adultCount);
                                    }
                                }
                            }
                        } catch (MalformedURLException e) {
                            System.err.println();
                            System.err.println("Invalid URL found in dmoz data: " + urlString);
                            System.err.println("Exception is " + e.getMessage());
                        } catch (IllegalArgumentException e) {
                            System.err.println();
                            System.err.println("Invalid URL found in dmoz data: " + urlString);
                            System.err.println("Exception is " + e.getMessage());
                        }
                    }
                } else if (epsMatcher.reset(curLine).matches()) {
                    urlString = epsMatcher.group(1);
                    inExternalPage = true;
                }
            }

            System.out.println("Processed " + numLines + " lines of input");
            
            for (String hostname : domains.keySet()) {
                Integer adultCount = domains.get(hostname);
                if (adultCount > 0) {
                    outputStream.println(hostname + "\t" + adultCount);
                }
            }
            System.out.println("Found " + numAdultUrls + " URLs");
        } catch (Exception e) {
            System.err.println("Exception while running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
