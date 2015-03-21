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
