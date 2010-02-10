package bixo.cascading;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings("unchecked")
public class BixoFlowProcess extends FlowProcess {
    private static final Logger LOGGER = Logger.getLogger(BixoFlowProcess.class);

    // TODO KKr - extend HadoopFlowProces, use Reporter.NULL for reporter, etc.
    // would
    // be safer than relying on never casting this to a HadoopFlowProcess when
    // in local
    // mode, which is fragile. Though better might be for FlowProcess to support
    // an isLocal() call, and a getReporter() call, where it returns null for
    // getReporter
    // when running local or if it's not a HadoopFlowProcess.
    private class FakeFlowProcess extends FlowProcess {

        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public void increment(Enum counter, int amount) {
        }

        @Override
        public void increment(String group, String counter, int amount) {
        }
        
        @Override
        public void keepAlive() {
        }

        @Override
        public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
            return null;
        }

        @Override
        public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
            return null;
        }

        @Override
        public void setStatus(String status) {
        }
    }

    private class HadoopFlowReporter implements IFlowReporter {
        private Reporter _reporter;

        public HadoopFlowReporter(Reporter reporter) {
            _reporter = reporter;
        }

        @Override
        public void setStatus(Level level, String msg) {
            if ((_reporter != null) && level.isGreaterOrEqual(Level.INFO)) {
                _reporter.setStatus("Cascading " + level + ": " + msg);
            }
        }

        @Override
        public void setStatus(String msg, Throwable t) {
            // TODO KKr - add stringified <t> (maybe from Nutch?) to msg.
            if (_reporter != null) {
                _reporter.setStatus("Cascading " + Level.ERROR + ": " + msg);
            }
        }
    }

    private FlowProcess _baseProcess;
    private boolean _isLocal;
    private List<IFlowReporter> _reporters;
    private Map<Enum, AtomicInteger> _localCounters;

    public BixoFlowProcess(FlowProcess baseProcess, IFlowReporter reporter) {
        init(baseProcess, reporter);
    }

    public BixoFlowProcess(HadoopFlowProcess baseProcess) {
        IFlowReporter reporter = new HadoopFlowReporter(baseProcess.getReporter());
        init(baseProcess, reporter);
    }

    /**
     * A no-argument constructor for use during testing, when we don't have a
     * real Cascading FlowProcess to use.
     */
    public BixoFlowProcess() {
        init(new FakeFlowProcess(), new LoggingFlowReporter());
    }

    private void init(FlowProcess baseProcess, IFlowReporter reporter) {
        _baseProcess = baseProcess;
        _isLocal = !(baseProcess instanceof HadoopFlowProcess)
                        || ((HadoopFlowProcess) baseProcess).getJobConf().get("mapred.job.tracker")
                                        .equalsIgnoreCase("local");

        _localCounters = new HashMap<Enum, AtomicInteger>();
        _reporters = new ArrayList<IFlowReporter>();
        addReporter(reporter);
    }

    public void addReporter(IFlowReporter reporter) {
        _reporters.add(reporter);
    }

    public void setStatus(String msg) {
        setStatus(Level.INFO, msg);
    }

    public void setStatus(String msg, Throwable t) {
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(msg, t);
        }
    }

    public void setStatus(Level level, String msg) {
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(level, msg);
        }
    }

    @Override
    public void increment(Enum counter, int amount) {
        _baseProcess.increment(counter, amount);

        // TODO KKr - decide if I really want to track stuff locally
        if (true || _isLocal) {
            synchronized (_localCounters) {
                if (_localCounters.get(counter) == null) {
                    _localCounters.put(counter, new AtomicInteger());
                }
            }

            AtomicInteger curCount = _localCounters.get(counter);
            int newValue = curCount.addAndGet(amount);

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Cascading counter: " + counter + (amount > 0 ? " + " : " - ")
                                + Math.abs(amount) + " = " + newValue);
            }
        }
    }

    @Override
    public void increment(String group, String counter, int amount) {
        _baseProcess.increment(group, counter, amount);
        
        // TODO KKr - get my local counters in sync?
    }

    public void decrement(Enum counter, int amount) {
        increment(counter, -amount);
    }

    public int getCounter(Enum counter) {
        // TODO KKr - figure out if I want to use my local counter here
        if (true || _isLocal) {
            AtomicInteger count = _localCounters.get(counter);
            if (count != null) {
                return count.get();
            } else {
                return 0;
            }
        } else {
            // TODO KKr - verify that this is the right way to get a counter
            // value, especially the part of
            // mapping from the Enum to the group/name pair needed for
            // reporter.getCounter().
            Reporter reporter = ((HadoopFlowProcess) _baseProcess).getReporter();

            // TODO KKr - on EC2, it looks like counter.getDeclaringClass()
            // returns null, as I get a NPE here
            // and I've verified that reporter is not null. But looking at the
            // Enum source, I don't see how
            // that could be unless counter.getClass().getSuperclass() returns
            // null.
            Counter hadoopCounter = null; // TODO KKr - getCounter() doesn't
                                          // exist with Bixo 0.18.3
                                          // reporter.getCounter(counter.getDeclaringClass().getName(),
                                          // counter.toString());
            if (hadoopCounter != null) {
                return (int)hadoopCounter.getCounter();
            } else {
                return 0;
            }
        }
    }

    /**
     * If we're running in local mode, log current counter values.
     */
    public void dumpCounters() {
        if (_isLocal) {
            for (Enum theEnum : _localCounters.keySet()) {
                LOGGER.info(String.format("Cascading counter: %s = %d", theEnum, _localCounters
                                .get(theEnum).get()));
            }
        }

        // FUTURE KKr - also dump Hadoop counters to Logger?
    }

    @Override
    public Object getProperty(String key) {
        return _baseProcess.getProperty(key);
    }

    @Override
    public void keepAlive() {
        _baseProcess.keepAlive();
    }

    @Override
    public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
        return _baseProcess.openTapForRead(tap);
    }

    @Override
    public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
        return _baseProcess.openTapForWrite(tap);
    }


}
