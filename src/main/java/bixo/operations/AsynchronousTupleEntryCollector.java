package bixo.operations;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * @author Nathan Summers
 *
 */
public class AsynchronousTupleEntryCollector extends TupleEntryCollector {
    private TupleEntryCollector collector;
    private BlockingQueue<Operation> operations = new LinkedBlockingQueue<Operation>();
    private Thread thread;
    private AtomicInteger operationThreads = new AtomicInteger();
    private Semaphore semaphore = new Semaphore(1);
    
    public AsynchronousTupleEntryCollector(String threadName) {
        semaphore.acquireUninterruptibly();
        
        thread = new Thread(threadName) {
            public void run() {                
                try {
                    while (true) {
                        Operation op = operations.take();

                        try {
                            waitForCollection();

                            op.apply();

                        } finally {
                            doneCollecting();
                        }

                    }
                } catch (InterruptedException e) {
                    try {
                        waitForCollection();
                        
                        for (Operation op = operations.poll(); op != null; op = operations.poll()) {
                            op.apply();
                        }
                    } finally {
                        doneCollecting();
                    }
                }
            }
        };
        
        thread.start();
    }

    public void setCollector(TupleEntryCollector collector) {
        this.collector = collector;
    }

    public void add(Tuple tuple) {
        operations.add(new AddTuple(new Tuple(tuple)));
    }

    public void add(TupleEntry tupleEntry) {
        operations.add(new AddEntry(new TupleEntry(tupleEntry)));
    }

    public void close() {
        operations.add(new Close());
    }

    public boolean equals(Object obj) {
        return collector.equals(obj);
    }

    public void setFields(Fields declared) {
        operations.add(new SetFields(declared));
    }

    @Override
    protected void collect(TupleEntry entry) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    public void allowCollection() {
        int operations = operationThreads.getAndIncrement();
        
        if (operations == 0) {
            semaphore.release();
        }
    }

    public void pauseCollection() {
        int operations = operationThreads.decrementAndGet();
        
        if (operations == 0) {
            semaphore.acquireUninterruptibly();
        } 
    }

    /**
     * Call when this is finished.
     */
    private void finished() {
        while (thread.isAlive()) {
            // We interrupt several times in case the interrupts get swallowed by buggy library code that doens't handle interrupts correctly
            thread.interrupt();
            try {
                thread.join(200);
            } catch (InterruptedException e) {
                // re-set interruption state
                Thread.currentThread().interrupt();
            }
        }
    }

    public void finishCollection() {
        finished();
        pauseCollection();
    }

    private void waitForCollection() {
        semaphore.acquireUninterruptibly();
    }

    private void doneCollecting() {
        semaphore.release();
    }

    private abstract class Operation {
        public abstract void apply();
    }

    private class SetFields extends Operation {
        private Fields declared;

        public SetFields(Fields declared) {
            this.declared = declared;
        }
    
        public void apply() {
            collector.setFields(declared);
        }
    
    }

    private class AddEntry extends Operation {
        @SuppressWarnings("hiding")
        private TupleEntry tupleEntry;

        public AddEntry(TupleEntry tupleEntry) {
            this.tupleEntry = tupleEntry;
        }
    
        public void apply() {
            collector.add(tupleEntry);
        }
    
    }

    private class AddTuple extends Operation {
        private Tuple tuple;

        public AddTuple(Tuple tuple) {
            this.tuple = tuple;
        }
    
        public void apply() {
            collector.add(tuple);
        }
    }

    private class Close extends Operation {
        public void apply() {
            collector.close();
        }
    }
}
