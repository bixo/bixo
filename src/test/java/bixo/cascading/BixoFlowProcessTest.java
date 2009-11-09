package bixo.cascading;

import org.junit.Assert;
import org.junit.Test;

import bixo.fetcher.FetcherCounters;

public class BixoFlowProcessTest {
    
    private enum TestCounter {
        COUNTER_A,
        COUNTER_B
    }
    
    @Test
    public void testLocalCounter() {
        BixoFlowProcess process = new BixoFlowProcess();        
        process.increment(TestCounter.COUNTER_A, 2);
        
        Assert.assertEquals(2, process.getCounter(TestCounter.COUNTER_A));
        Assert.assertEquals(0, process.getCounter(TestCounter.COUNTER_B));

        process.decrement(TestCounter.COUNTER_B, 2);
        Assert.assertEquals(-2, process.getCounter(TestCounter.COUNTER_B));
    }
    
    @Test
    public void testHadoopCounter() {
        
        Enum<FetcherCounters> counter = FetcherCounters.URLS_FETCHING;
        String groupName = counter.getDeclaringClass().getName();
        String counterName = counter.toString();
        System.out.println(groupName + " / " + counterName);
        // TODO KKr - how to test "real" Hadoop counters? Need to be running in non-local
        // mode, with a real Cascading flow.
    }
}
