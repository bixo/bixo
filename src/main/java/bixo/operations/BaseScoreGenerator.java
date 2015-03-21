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
package bixo.operations;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import bixo.datum.GroupedUrlDatum;
import bixo.utils.DomainNames;

@SuppressWarnings("serial")
public abstract class BaseScoreGenerator implements Serializable {
    public static final double SKIP_SCORE = Double.NEGATIVE_INFINITY;

    /**
     * Return whether the domain should be crawled.
     * 
     * @param domain host from URL
     * @param pld paid-level domain
     * @return true if the domain should be crawled.
     */
    public boolean isGoodDomain(String domain, String pld) {
        String url = null;
        return generateScore(domain, pld, url) != SKIP_SCORE;
    }
    
    
    public double generateScore(String url) {
        try {
            URL realUrl = new URL(url);
            String host = realUrl.getHost();
            return generateScore(host, DomainNames.getPLD(host), url);
        } catch (MalformedURLException e) {
            return SKIP_SCORE;
        }
    }

    public double generateScore(String domain, String pld, GroupedUrlDatum url) {
        return generateScore(domain, pld, url.getUrl());
    }

    /**
     * Return score for URL, based on domain & URL path
     * 
     * @param domain hostname
     * @param pld paid-level domain (derived from domain()
     * @param url URL, or null for scoring domain/pld
     * @return the generated score for the URL
     */
    abstract public double generateScore(String domain, String pld, String url);

}
