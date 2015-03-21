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
package bixo.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class DmozLinks {
    private static final String UNZIPPED_FILE_NAME = "dmoz-links.txt";

    private static final int BUFFER_SIZE = 32 * 1024;

    private static int TOTAL_LINKS = 4202774;
    
    private static int MAX_RANDOM_LINKS = 10000;
    private static int MIN_RANDOM_LINKS = 10;
    
    private ZipFile _dmozZipFile;
    private ZipEntry _linksEntry;
    private Random _rand;
    
    public DmozLinks(File dmozZipFile) throws ZipException, IOException {
        _dmozZipFile = new ZipFile(dmozZipFile);
        _linksEntry = _dmozZipFile.getEntry(UNZIPPED_FILE_NAME);
        if (_linksEntry == null) {
            throw new IllegalArgumentException("Zip file does not contain required file: " + UNZIPPED_FILE_NAME);
        }
        
        _rand = new Random();
    }
    
    public List<String> getRandomLinks(int numLinks) throws UnsupportedEncodingException, IOException {
        if (numLinks < MIN_RANDOM_LINKS) {
            throw new IllegalArgumentException("Minimum number of random links is " + MIN_RANDOM_LINKS);
        } else if (numLinks > MAX_RANDOM_LINKS) {
            throw new IllegalArgumentException("Maximum number of random links is " + MAX_RANDOM_LINKS);
        }
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(_dmozZipFile.getInputStream(_linksEntry), "UTF-8"), BUFFER_SIZE);
        ArrayList<String> result = new ArrayList<String>(numLinks);

        // We want numLinks out of TOTAL_LINKS URLs. So we'll pick a value to use as the mod of the hash
        // such that roughly the right number of links get picked for 1 out of <mod> values. Actually we'll
        // shoot high, so we can terminate early when we have enough, versus not getting enough entries.
        // <numLinks>/TOTAL_LINKS = 100/<mod>. <mod> = 100 * TOTAL_LINKS/<numLinks>
        
        int mod = (100 * TOTAL_LINKS) / numLinks;
        int randXor = _rand.nextInt();
        
        try {
            while (result.size() < numLinks) {
                String curLine = reader.readLine();
                if (curLine == null) {
                    break;
                }
                
                // TODO KKr - recalculate mod, based on whenever we've processed half of
                // the remaining links. The mod calc should be based on remaining links
                // (TOTAL_LINKS - links seen) and links still needed (numLinks - result size).
                int hash = Math.abs(curLine.hashCode() ^ randXor);
                int modResult = hash % mod;
                if (modResult < 100) {
                    result.add("http://" + curLine);
                }
            }
        } finally {
            reader.close();
        }
        
        return result;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            DmozLinks links = new DmozLinks(new File(args[0]));
            
            List<String> result = links.getRandomLinks(1000);
            System.out.println("Got " + result.size() + " links");
            for (String link : result) {
                System.out.println(link);
            }
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
