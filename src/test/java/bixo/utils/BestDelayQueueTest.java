package bixo.utils;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import org.junit.Test;


public class BestDelayQueueTest {
    private static class DelayUntilTime implements Delayed {
        private long _targetTime;
        private int _value;
        
        public DelayUntilTime(long until, int value) {
            _targetTime = until;
            _value = value;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long result = _targetTime - System.currentTimeMillis();
            if (result <= 0) {
                result = -_value;
            }
            
            return unit.convert(result, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            long _otherTime = ((DelayUntilTime)other)._targetTime;
            
            if (_targetTime < _otherTime) {
                return -1;
            } else if (_targetTime > _otherTime) {
                return 1;
            } else {
                return 0;
            }
        }

        public int getValue() {
            return _value;
        }
    }
    
    @Test
    public void testNoElements() {
        DelayQueue<DelayUntilTime> queue = new BestDelayQueue<DelayUntilTime>();
        
        assertNull(queue.poll());
    }
    
    @Test
    public void testNoElementReady() {
        DelayQueue<DelayUntilTime> queue = new BestDelayQueue<DelayUntilTime>();
        
        long curTime = System.currentTimeMillis();
        DelayUntilTime e1 = new DelayUntilTime(curTime + 100, 5);
        queue.offer(e1);

        assertNull(queue.poll());
    }
    
    @Test
    public void testManyElementsReady() {
        DelayQueue<DelayUntilTime> queue = new BestDelayQueue<DelayUntilTime>();
        
        long curTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            DelayUntilTime e = new DelayUntilTime(curTime, i);
            queue.offer(e);
        }
        
        for (int i = 9; i >= 0; i--) {
            assertEquals(i, queue.poll().getValue());
        }
        
        assertNull(queue.poll());
    }
    
    @Test
    public void testDynamicDelay() throws InterruptedException {
        DelayQueue<DelayUntilTime> queue = new BestDelayQueue<DelayUntilTime>();
        
        long curTime = System.currentTimeMillis();
        DelayUntilTime e1 = new DelayUntilTime(curTime + 5, 5);
        queue.offer(e1);
        
        DelayUntilTime e2 = new DelayUntilTime(curTime + 12, 50);
        queue.offer(e2);
        
        DelayUntilTime e3 = new DelayUntilTime(curTime + 7, 2);
        queue.offer(e3);
        
        Thread.sleep(15);

        assertEquals(e2, queue.poll());
        assertEquals(e1, queue.poll());
        assertEquals(e3, queue.poll());
    }

}
