package bixo.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

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
    
    private static class PollQueueRunnable implements Runnable {
        
        private DelayQueue<DelayUntilTime> _queue;
        private long _startTime;
        private boolean _done;
        private boolean _gotEntry;
        
        public PollQueueRunnable(DelayQueue<DelayUntilTime> q, long startTime) {
            _queue = q;
            _startTime = startTime;
            _done = false;
            _gotEntry = false;
        }

        @Override
        public void run() {
            while (System.currentTimeMillis() <= _startTime) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            _gotEntry = _queue.poll() != null;
            _done = true;
        }
        
        public boolean isDone() {
            return _done;
        }
        
        public boolean gotEntry() {
            return _gotEntry;
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
    
    @Test
    public void testThreadContention() throws InterruptedException {
        DelayQueue<DelayUntilTime> queue = new BestDelayQueue<DelayUntilTime>();
        
        final int numElements = 100;
        
        long curTime = System.currentTimeMillis();
        for (int i = 0; i < numElements; i++) {
            DelayUntilTime element = new DelayUntilTime(curTime, i);
            assertTrue(queue.offer(element));
        }
        
        long startTime = System.currentTimeMillis() + 10;
        PollQueueRunnable runnables[] = new PollQueueRunnable[100];
        for (int i = 0; i < numElements; i++) {
            runnables[i] = new PollQueueRunnable(queue, startTime);
            Thread t = new Thread(runnables[i]);
            t.start();
        }
        
        while (System.currentTimeMillis() < startTime) {
            Thread.sleep(1);
        }
        
        synchronized (queue) {
            Iterator<DelayUntilTime> iter = queue.iterator();
            while (iter.hasNext()) {
                iter.next();
            }
        }

        boolean allDone = false;
        while (!allDone) {
            allDone = true;
            for (int i = 0; i < numElements; i++) {
                if (!runnables[i].isDone()) {
                    allDone = false;
                    break;
                } else {
                    assertTrue(runnables[i].gotEntry());
                }
            }
        }
        
    }

}
