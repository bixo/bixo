package bixo.utils;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;


public class DiskQueueTest {

    @Test
    public void testQueue() {
        DiskQueue<String> queue = new DiskQueue<String>(1);
        
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
        DiskQueue<String> queue = new DiskQueue<String>(1);
        
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
        DiskQueue<Integer> queue = new DiskQueue<Integer>(10);
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
        DiskQueue<Integer> queue = new DiskQueue<Integer>(1);

        // We'll repeatedly overflow to disk, and then drain the queue,
        // thus forcing file creation/deletion.
        for (int i = 0; i < 100; i++) {
            assertTrue(queue.offer(new Integer(666)));
            assertTrue(queue.offer(new Integer(333)));
            assertEquals(666, queue.remove().intValue());
            assertEquals(333, queue.remove().intValue());
            assertNull(queue.poll());
        }
    }
    
    @Test
    public void testInvalidQueueSize() {
        try {
            new DiskQueue<String>(0);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // valid
        }
    }
}
