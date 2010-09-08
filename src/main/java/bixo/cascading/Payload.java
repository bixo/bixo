package bixo.cascading;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class Payload implements Map<String, Object>, Serializable {

    private Map<String, Object> _data;
    
    public Payload() {
        _data = new HashMap<String, Object>();
    }

    public Payload(Payload p) {
        this();
        
        _data.putAll(p._data);
    }
    
    public Payload(Tuple tuple) {
        this();
        
        int tupleSize = (tuple == null ? 0 : tuple.size());
        int numEntries = tupleSize / 2;
        if (numEntries * 2 != tupleSize) {
            throw new RuntimeException("Payload tuple has odd size");
        }
        
        int offset = 0;
        for (int i = 0; i < numEntries; i++) {
            String key = (String)tuple.getString(offset++);
            Object value = tuple.getObject(offset++);
            put(key, value);
        }
    }
    
    public void clear() {
        _data.clear();
    }

    public boolean containsKey(Object key) {
        return _data.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return _data.containsValue(value);
    }

    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return _data.entrySet();
    }

    public boolean equals(Object o) {
        return _data.equals(o);
    }

    public Object get(Object key) {
        return _data.get(key);
    }

    public int hashCode() {
        return _data.hashCode();
    }

    public boolean isEmpty() {
        return _data.isEmpty();
    }

    public Set<String> keySet() {
        return _data.keySet();
    }

    public Object put(String key, Object value) {
        return _data.put(key, value);
    }

    public void putAll(Map<? extends String, ? extends Object> t) {
        _data.putAll(t);
    }

    public Object remove(Object key) {
        return _data.remove(key);
    }

    public int size() {
        return _data.size();
    }

    public Collection<Object> values() {
        return _data.values();
    }
    
    public Tuple toTuple() {
        Tuple result = new Tuple();
        
        for (Entry<String, Object> entry : _data.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue());
        }
        
        return result;
    }
}
