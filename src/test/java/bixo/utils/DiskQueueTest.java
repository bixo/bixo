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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import bixo.datum.FetchSetDatum;
import bixo.datum.ScoredUrlDatum;
import bixo.datum.UrlStatus;

public class DiskQueueTest {

    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }
    
    private static class AtomicIntegerComparator implements Comparator<AtomicInteger> {

        @Override
        public int compare(AtomicInteger o1, AtomicInteger o2) {
            return o1.get() - o2.get();
        }
    }
    
    private static class IntegerComparator implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2;
        }
    }
    
    private static class FetchSetComparator implements Comparator<FetchSetDatum> {
        
        @Override
        public int compare(FetchSetDatum o1, FetchSetDatum o2) {
            if (o1.getFetchTime() < o2.getFetchTime()) {
                return -1;
            } else if (o1.getFetchTime() > o2.getFetchTime()) {
                return 1;
            } else if (o1.getUrls().size() > o2.getUrls().size()) {
                return -1;
            } else if (o1.getUrls().size() < o2.getUrls().size()) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    
    @Test
    public void testQueue() {
        DiskQueue<String> queue = new DiskQueue<String>(1, new StringComparator());
        
        assertTrue(queue.offer("one"));
        assertTrue(queue.offer("two"));
        
        assertEquals("one", queue.remove());
        assertEquals("two", queue.remove());
        assertNull(queue.poll());
        
        assertTrue(queue.offer("three"));
        assertEquals("three", queue.remove());
        assertNull(queue.poll());
    }
    
    @Test
    public void testReadAndWriteFromDiskQueue() {
        DiskQueue<String> queue = new DiskQueue<String>(1, new StringComparator());
        
        assertTrue(queue.offer("one"));
        assertTrue(queue.offer("two"));
        assertTrue(queue.offer("three"));
        
        assertEquals("one", queue.remove());
        assertTrue(queue.offer("four"));
        assertEquals("two", queue.remove());
        assertEquals("three", queue.remove());
        assertEquals("four", queue.remove());
        assertNull(queue.poll());
    }
    
    @Test
    public void testLotsOfOperations() {
        DiskQueue<Integer> queue = new DiskQueue<Integer>(10, new IntegerComparator());
        int numInQueue = 0;
        int readIndex = 0;
        int writeIndex = 0;
        
        Random rand = new Random(137);
        
        for (int i = 0; i < 10000; i++) {
            if (rand.nextInt(10) < 4) {
                assertEquals(readIndex, queue.remove().intValue());
                readIndex += 1;
                numInQueue -= 1;
            } else {
                assertTrue(queue.offer(new Integer(writeIndex)));
                writeIndex += 1;
                numInQueue += 1;
            }
        }
        
        while (numInQueue > 0) {
            assertEquals(readIndex, queue.remove().intValue());
            readIndex += 1;
            numInQueue -= 1;
        }
    }
    
    @Test
    public void testLotsOfFileDeleteAndCreate() {
        DiskQueue<Integer> queue = new DiskQueue<Integer>(1, new IntegerComparator());

        // We'll repeatedly overflow to disk, and then drain the queue,
        // thus forcing file creation/deletion.
        for (int i = 0; i < 100; i++) {
            assertTrue(queue.offer(new Integer(333)));
            assertTrue(queue.offer(new Integer(666)));
            assertEquals(333, queue.remove().intValue());
            assertEquals(666, queue.remove().intValue());
            assertNull(queue.poll());
        }
    }
    
    @Test
    public void testInvalidQueueSize() {
        try {
            new DiskQueue<String>(0, new StringComparator());
            fail("Should have thrown exception");
        } catch (Exception e) {
            // valid
        }
    }
    
    @Test
    public void testClearingQueue() {
        DiskQueue<Integer> queue = new DiskQueue<Integer>(1, new IntegerComparator());
        assertTrue(queue.offer(new Integer(666)));
        queue.clear();
        
        assertEquals(0, queue.size());
        assertNull(queue.poll());
    }
    
    @Test
    public void testSizeWithCachedInternalEntry() {
        // Tricky test to expose subtle bug. If we have a cached entry (read from
        // disk, but not in queue) this needs to get accounted for in the size
        // calculation.
        
        DiskQueue<Integer> queue = new DiskQueue<Integer>(1, new IntegerComparator());
        assertTrue(queue.offer(new Integer(1)));
        assertTrue(queue.offer(new Integer(2)));
        assertTrue(queue.offer(new Integer(3)));
        
        assertEquals(1, queue.remove().intValue());
        assertEquals(2, queue.remove().intValue());
        assertEquals(1, queue.size());
    }
    
    @Test
    public void testReleasingMemory() {
        DiskQueue<String> queue = new DiskQueue<String>(1, new StringComparator());

        assertTrue(queue.offer("first"));
        assertTrue(queue.offer("second"));
        try {
            for (int i = 0; i < 30; i++) {
                byte data[] = new byte[1000000];
                assertTrue(queue.offer(new String(data)));
                assertNotNull(queue.poll());
            }
        } finally {
            queue.clear();
        }
    }
    
    @Test
    public void testGettingBackWhatWasWritten() {
        final int numElements = 100;
        DiskQueue<FetchSetDatum> queue = new DiskQueue<FetchSetDatum>(numElements/10, new FetchSetComparator());
        
        final long fetchStartTime = System.currentTimeMillis();
        final long fetchDelay = 30000;
        FetchSetDatum datums[] = new FetchSetDatum[numElements];
        for (int i = 0; i < numElements; i++) {
            long fetchTime = fetchStartTime + (i * 10);
            int groupingKey = 100;
            String groupingRef = "groupingRef";
            List<ScoredUrlDatum> scoredUrls = new ArrayList<ScoredUrlDatum>();
            String url = String.format("http://domain-%03d.com/index.html", i);
            scoredUrls.add(new ScoredUrlDatum(url, groupingRef, UrlStatus.UNFETCHED, 0.0));
            FetchSetDatum datum = new FetchSetDatum(scoredUrls, fetchTime, fetchDelay, groupingKey, groupingRef);
            datums[i] = datum;
            assertTrue(queue.offer(datum));
        }
        
        for (int i = 0; i < numElements; i++) {
            FetchSetDatum datum = queue.poll();
            assertNotNull(datum);
            assertEquals(datums[i], datum);
        }
        
        assertNull(queue.poll());
        
    }

    @Test
    public void testAddingNullElement() {
        DiskQueue<Integer> queue = new DiskQueue<Integer>(1, new IntegerComparator());

        try {
            queue.offer(null);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // valid
        }
        
        // Now test when adding null that gets written to disk.
        assertTrue(queue.offer(new Integer(666)));
        assertTrue(queue.offer(new Integer(666)));
        
        try {
            queue.offer(null);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // valid
        }
    }
    
    @Test
    public void testPeekingAndGetting() throws Exception {
        DiskQueue<Integer> queue = new DiskQueue<Integer>(3, new IntegerComparator());
        
        assertTrue(queue.offer(new Integer(666)));
        assertTrue(queue.offer(new Integer(999)));
        assertTrue(queue.offer(new Integer(333)));
        assertEquals(333, queue.remove().intValue());
        assertEquals(2, queue.size());

        assertEquals(666, queue.remove().intValue());
        assertEquals(1, queue.size());

        assertEquals(999, queue.remove().intValue());
        assertEquals(0, queue.size());
    }
    
    @Test
    public void testDynamicSortOrder() throws Exception {
        DiskQueue<AtomicInteger> queue = new DiskQueue<AtomicInteger>(3, new AtomicIntegerComparator());

        AtomicInteger i1 = new AtomicInteger(0);
        AtomicInteger i2 = new AtomicInteger(10);
        assertTrue(queue.offer(i1));
        assertTrue(queue.offer(i2));
        
        assertEquals(i1.get(), queue.peek().get());

        // Change the value of the first item, and verify that we now
        // get the new lowest value.
        i1.set(100);
        assertEquals(i2.get(), queue.peek().get());
    }
    
    @Test
    public void testPeeking() throws Exception {
        DiskQueue<AtomicInteger> queue = new DiskQueue<AtomicInteger>(3, new AtomicIntegerComparator());
        assertEquals(null, queue.peek());
    }
    
}
