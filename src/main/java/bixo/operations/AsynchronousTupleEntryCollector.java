package bixo.operations;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

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
    
    public AsynchronousTupleEntryCollector(String threadName, final Semaphore semaphore) {        
        thread = new Thread(threadName) {
            public void run() {                
                try {
                    while (true) {
                        Operation op = operations.take();
                        try {
                            semaphore.acquireUninterruptibly();
                            
                            for (; op != null; op = operations.poll()) {
                                op.apply();
                            }
                        } finally {
                            semaphore.release();
                        }
                    }
                } catch (InterruptedException e) {
                    try {
                        //finish the queue before exiting.
                        semaphore.acquireUninterruptibly();
                        
                        for (Operation op = operations.poll(); op != null; op = operations.poll()) {
                            op.apply();
                        }
                    } finally {
                        semaphore.release();
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

    /**
     * Call when this is finished.
     */
    public void finished() {
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            // re-set interruption state
            Thread.currentThread().interrupt();
        }
    }
}
