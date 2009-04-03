package bixo.fetcher.impl;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;

import bixo.fetcher.impl.LastFetchScoreGenerator;
import bixo.items.UrlItem;

public class LastFetchScoreGeneratorTest {

    @Test
    public void testScore() throws Exception {
        long now = System.currentTimeMillis();
        LastFetchScoreGenerator generator = new LastFetchScoreGenerator(now, 10);
        UrlItem mock = Mockito.mock(UrlItem.class);
        Mockito.when(mock.getLastFetched()).thenReturn(0l);
        Assert.assertEquals(1.0, generator.generateScore(mock));
        Mockito.when(mock.getLastFetched()).thenReturn(now - 11);
        Assert.assertEquals(1.0, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now);
        Assert.assertEquals(0.0, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 5);
        Assert.assertEquals(0.5, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 1);
        Assert.assertEquals(0.1, generator.generateScore(mock));

        Mockito.when(mock.getLastFetched()).thenReturn(now - 9);
        Assert.assertEquals(0.9, generator.generateScore(mock));
    }
}
