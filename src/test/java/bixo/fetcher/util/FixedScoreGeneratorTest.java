package bixo.fetcher.util;

import junit.framework.Assert;

import org.junit.Test;


public class FixedScoreGeneratorTest {

    @Test
    public void testConstantScore() throws Exception {
        final double score = 50.0;
        FixedScoreGenerator fsg = new FixedScoreGenerator(score);
        
        Assert.assertEquals(score, fsg.generateScore("www.domain.com", "domain.com", "http://domain.com"));
    }
}
