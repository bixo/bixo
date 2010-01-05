package bixo.utils;

import java.util.concurrent.RejectedExecutionException;

import junit.framework.Assert;

import org.junit.Test;


public class ThreadedExecutorTest {

    @Test
    public void testNoRejection() {
        final long timeoutInMS = 10;
        
        ThreadedExecutor executor = new ThreadedExecutor(1, timeoutInMS);
        
        for (int i = 0; i < 100; i++) {
            try {
                Runnable cmd = new Runnable() {
                    public void run() {
                        try {
                            Thread.sleep(timeoutInMS/2);
                        } catch (InterruptedException e) {
                            // Terminate the run
                        }
                    }
                };
                
                executor.execute(cmd);
            } catch (RejectedExecutionException e) {
                Assert.fail("Execution was rejected");
            }
        }
    }
    
    @Test
    public void testNoRejectionMultiThreaded() {
        final long timeoutInMS = 10;
        final int numThreads = 2;
        
        ThreadedExecutor executor = new ThreadedExecutor(numThreads, timeoutInMS);
        for (int i = 0; i < (numThreads + 1); i++) {
            try {
                Runnable cmd = new Runnable() {
                    public void run() {
                        try {
                            Thread.sleep(timeoutInMS - 1);
                        } catch (InterruptedException e) {
                            // Terminate the run
                        }
                    }
                };
                
                // The key is the <numThreads + 1> request. This should block
                // until one of the threads has completed.
                long startTime = System.currentTimeMillis();
                executor.execute(cmd);
                long deltaTime = System.currentTimeMillis() - startTime;
                System.out.println("Request took: " + deltaTime);
            } catch (RejectedExecutionException e) {
                Assert.fail("Execution was rejected");
            }
        }
    }
    
    @Test
    public void testRejection() {
        final long timeoutInMS = 4;
        
        ThreadedExecutor executor = new ThreadedExecutor(1, timeoutInMS);
        
        try {
            Runnable cmd = new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(timeoutInMS * 5);
                    } catch (InterruptedException e) {
                        // Terminate the run
                    }
                }
            };
            
            // This first call should work.
            executor.execute(cmd);
        } catch (RejectedExecutionException e) {
            Assert.fail("Execution was rejected");
        }
        
        try {
            Runnable cmd = new Runnable() {
                public void run() { }
            };
            
            // This call should fail, since the previous Runnable isn't done yet.
            executor.execute(cmd);
            Assert.fail("Should have failed");
        } catch (RejectedExecutionException e) {
            // Valid
        }
    }
    
    @Test
    public void testNormalTermination() {
        final long timeoutInMS = 100;

        ThreadedExecutor executor = new ThreadedExecutor(1, timeoutInMS);

        try {
            Runnable cmd = new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(timeoutInMS/2);
                    } catch (InterruptedException e) {
                        // Terminate the run
                    }
                }
            };

            executor.execute(cmd);
            
            Assert.assertTrue(executor.terminate());
        } catch (RejectedExecutionException e) {
            Assert.fail("Execution was rejected");
        } catch (InterruptedException e) {
            Assert.fail("Termination was interrupted");
        }
    }

    @Test
    public void testHardTermination() {
        final long timeoutInMS = 50;

        ThreadedExecutor executor = new ThreadedExecutor(1, timeoutInMS);

        try {
            Runnable cmd = new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(timeoutInMS * 10);
                    } catch (InterruptedException e) {
                        // Terminate the run
                    }
                }
            };

            executor.execute(cmd);
            
            Assert.assertFalse(executor.terminate());
        } catch (RejectedExecutionException e) {
            Assert.fail("Execution was rejected");
        } catch (InterruptedException e) {
            Assert.fail("Termination was interrupted");
        }
    }
}
