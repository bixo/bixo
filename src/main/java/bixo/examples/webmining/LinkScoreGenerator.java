/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import bixo.datum.GroupedUrlDatum;
import bixo.operations.BaseScoreGenerator;

@SuppressWarnings("serial")
public class LinkScoreGenerator extends BaseScoreGenerator {

    public static final double DEFAULT_SCORE = 1.0;

    public LinkScoreGenerator() {
    }
    
    @Override
    public double generateScore(String domain, String pld, GroupedUrlDatum url) {
        // Since we limit the number of urls to be fetched per loop, 
        // check the flag in payload and set skip score if skip flag is set.
        boolean skipped = (Boolean)url.getPayloadValue(CustomFields.SKIP_BY_LIMIT_FN);
        if (skipped) {
            return SKIP_SCORE;
        } else {
            double linkScore = ((Float)url.getPayloadValue(CustomFields.LINKS_SCORE_FN)).doubleValue();
            return linkScore;
        }
    }

    @Override
    public double generateScore(String domain, String pld, String url) {
        return DEFAULT_SCORE;
    }

}
