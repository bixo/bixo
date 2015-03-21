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

import java.util.concurrent.RejectedExecutionException;

import junit.framework.Assert;

import org.junit.Test;


public class ThreadedExecutorTest {

    @Test
    public void testNoRejection() {
        final long timeoutInMS = 100;

        ThreadedExecutor executor = new ThreadedExecutor(1, timeoutInMS);

        for (int i = 0; i < 10; i++) {
            try {
                Runnable cmd = new Runnable() {
                    public void run() {
                        try {
                            Thread.sleep(timeoutInMS/4);
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
                            // Sleep for a bit less than the max timeout, which means the
                            // first <numThreads> requests will queue up, and the third
                            // one will wait until one of those two frees up.
                            Thread.sleep(timeoutInMS - 2);
                        } catch (InterruptedException e) {
                            // Terminate the run
                        }
                    }
                };
                
                // The key is the <numThreads + 1> request. This should block
                // until one of the threads has completed.
                executor.execute(cmd);
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
            
            Assert.assertTrue(executor.terminate(timeoutInMS));
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
            
            Assert.assertFalse(executor.terminate(timeoutInMS));
        } catch (RejectedExecutionException e) {
            Assert.fail("Execution was rejected");
        } catch (InterruptedException e) {
            Assert.fail("Termination was interrupted");
        }
    }
}
