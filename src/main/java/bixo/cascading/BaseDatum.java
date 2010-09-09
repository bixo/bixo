package bixo.cascading;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public abstract class BaseDatum implements Serializable {
    
    protected TupleEntry _tupleEntry;
    
    public BaseDatum() {
        this(new Fields());
    }
    
    /**
     * Create an empty datum with field names defined by <fields>
     * 
     * @param fields Names of fields
     */
    public BaseDatum(Fields fields) {
        this(new TupleEntry(fields, Tuple.size(fields.size())));
    }
    
    /**
     * Create a new datum with field names defined by <fields>, and
     * field values contained in <tuple>
     * 
     * WARNING - <tuple> will be kept as the data container, so don't call this
     * with a tuple provided by a Cascading operation/iterator, as those get reused.
     * 
     * @param fields Names of fields
     * @param tuple Data for the datum
     */
    public BaseDatum(Fields fields, Tuple tuple) {
        if (fields.size() != tuple.size()) {
            throw new IllegalArgumentException("Size of fields must be the same as the size of the tuple: " + fields + "/" + tuple);
        }
        
        _tupleEntry = new TupleEntry(fields, tuple);
    }
    

    public BaseDatum(TupleEntry tupleEntry) {
        _tupleEntry = tupleEntry;
    }
    
    /**
     * Set the data container to be <tupleEntry>
     * 
     * @param tupleEntry Data for the datum.
     */
    public void setTupleEntry(TupleEntry tupleEntry) {
        setTupleEntry(tupleEntry, true);
    }
    
    /**
     * Set the data container to be <tupleEntry>
     * 
     * @param tupleEntry Data for the datum.
     */
    protected void setTupleEntry(TupleEntry tupleEntry, boolean checkFields) {
        if (checkFields && !tupleEntry.getFields().equals(getFields())) {
            throw new IllegalArgumentException("Fields must be the same as for current value: " + tupleEntry.getFields() + "/" + _tupleEntry.getFields());
        }
        
        _tupleEntry = tupleEntry;
        reset();
    }
    
    public void setTuple(Tuple tuple) {
        if (getFields().size() != tuple.size()) {
            throw new IllegalArgumentException("Size of tuple doesn't match current fields");
        }
        
        _tupleEntry.setTuple(tuple);
        reset();
    }
    
    public Tuple getTuple() {
        return getTupleEntry().getTuple();
    }
    
    public TupleEntry getTupleEntry() {
        commit();
        
        return _tupleEntry;
    }
    
    public Fields getFields() {
        return _tupleEntry.getFields();
    }
    
    protected void validateFields(Fields superFields, Fields myFields) {
        if (!superFields.contains(myFields)) {
            throw new IllegalArgumentException("Fields passed to constructor don't contain " + myFields);
        }
    }
    
    protected void validateFields(TupleEntry tupleEntry, Fields myFields) {
        if (!tupleEntry.getFields().contains(myFields)) {
            throw new IllegalArgumentException("Fields passed to constructor don't contain " + myFields);
        }
    }
    
    // Provide way for subclasses to fix up _tupleEntry with in-memory data.
    public void commit() {};
    
    // Provide way for subclasses to fix up in-memory data when _tupleEntry changes.
    public void reset() {};
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_tupleEntry == null) ? 0 : _tupleEntry.hashCode());
        return result;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        // Make sure anything in memory has been flushed to _tupleEntry
        commit();
        
        TupleEntry te = getTupleEntry();
        s.writeObject(te.getFields());
        s.writeObject(te.getTuple());
    }
    
    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        Fields fields = (Fields)s.readObject();
        Tuple tuple = (Tuple)s.readObject();
        
        // Don't check field consistency, as serialization has created an empty Datum (no fields
        // defined) so they won't match up.
        setTupleEntry(new TupleEntry(fields, tuple), false);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        
        BaseDatum other = (BaseDatum) obj;
        if (_tupleEntry == null) {
            return other._tupleEntry == null;
        } else if (!_tupleEntry.getFields().equals(other._tupleEntry.getFields())) {
            return false;
        } else if (!_tupleEntry.getTuple().equals(other._tupleEntry.getTuple())) {
            return false;
        }
        
        return true;
    }

    @SuppressWarnings("unchecked")
    public static <T extends BaseDatum> T copy(T datum) {
        try {
            T result = (T)datum.getClass().newInstance();
            result.setTupleEntry(datum.getTupleEntry());
            return datum;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Create a unique field name
     * 
     * Combines the class name and the user-defined field name
     * 
     * @param clazz
     * @param field
     * @return
     */
    @SuppressWarnings("unchecked")
    public static String fieldName(Class clazz, String field) {
        return clazz.getSimpleName() + "-" + field;
    }
    
    @SuppressWarnings("unchecked")
    public static Fields getSuperFields(Class<? extends BaseDatum> clazz) {
        try {
            Class<?> superClass = clazz.getSuperclass();
            if (superClass.equals(BaseDatum.class)) {
                return new Fields();
            } else {
                Class<? extends BaseDatum> superClazz = (Class<? extends BaseDatum>)clazz.getSuperclass();
                BaseDatum datum = superClazz.newInstance();
                return datum.getFields();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
