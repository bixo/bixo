package bixo.cascading;

import static org.junit.Assert.*;

import org.apache.log4j.Level;
import org.junit.Test;

import bixo.cascading.BixoFlowProcess.LoggingLevels;
import bixo.hadoop.FetchCounters;

public class BixoFlowProcessTest {
    
    private enum TestCounter {
        COUNTER_A,
        COUNTER_B
    }
    
    @Test
    public void testLocalCounter() {
        BixoFlowProcess process = new BixoFlowProcess();        
        process.increment(TestCounter.COUNTER_A, 2);
        
        assertEquals(2, process.getCounter(TestCounter.COUNTER_A));
        assertEquals(0, process.getCounter(TestCounter.COUNTER_B));

        process.decrement(TestCounter.COUNTER_B, 2);
        assertEquals(-2, process.getCounter(TestCounter.COUNTER_B));
    }
    
    @Test
    public void testHadoopCounter() {
        
        Enum<FetchCounters> counter = FetchCounters.URLS_FETCHING;
        String groupName = counter.getDeclaringClass().getName();
        String counterName = counter.toString();
        System.out.println(groupName + " / " + counterName);
        // TODO KKr - how to test "real" Hadoop counters? Need to be running in non-local
        // mode, with a real Cascading flow.
    }
    
    @Test
    public void testLoggingCounter() {
        assertEquals(LoggingLevels.INFO, LoggingLevels.fromLevel(Level.INFO));
        assertEquals("INFO", LoggingLevels.INFO.toString());
    }
}
