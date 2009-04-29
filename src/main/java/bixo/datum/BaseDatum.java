package bixo.datum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("unchecked")
public abstract class BaseDatum {

    private HashMap<String, Comparable> _map;

    public BaseDatum(Map<String, Comparable> metaData) {
        _map = (HashMap<String, Comparable>) metaData;
    }

    public Comparable get(String key) {
        if (_map == null) {
            return null;
        } else {
            return _map.get(key);
        }
    }

    public void set(String key, Comparable value) {
        if (_map == null) {
            _map = new HashMap<String, Comparable>();
        }

        _map.put(key, value);
    }

    public Map<String, Comparable> getMap() {
        return _map;
    }

    /**
     * Create a tuple from the "standard" fields, plus all of the meta-data,
     * where the order of the meta-data values is based on String sort order for
     * the field names (map key values).
     * 
     * @return new Tuple
     */
    public Tuple toTuple() {
        Tuple tuple = new Tuple(getValues());
        if (_map != null) {
            String[] keys = _map.keySet().toArray(new String[_map.size()]);
            Arrays.sort(keys);
            for (String key : keys) {
                Comparable value = _map.get(key);
                tuple.add(value);
            }
        }
        return tuple;
    }

    protected abstract Comparable[] getValues();

    protected static Map<String, Comparable> extractMetaData(Tuple tuple, int startingOffset, Fields metaDataFieldNames) {

        ArrayList<Comparable> fieldNames = new ArrayList<Comparable>();
        Iterator iterator = metaDataFieldNames.iterator();

        while (iterator.hasNext()) {
            String fieldName = (String) iterator.next();
            fieldNames.add(fieldName);
        }

        Collections.sort(fieldNames);
        try {
            HashMap<String, Comparable> map = new HashMap<String, Comparable>();
            for (int i = 0; i < fieldNames.size(); i++) {
                String key = (String) fieldNames.get(i);
                Comparable value = tuple.get(startingOffset + i);
                map.put(key, value);
            }
            return map;

        } catch (Exception e) {
            System.out.println("BaseDatum.extractMetaData()");
        return null;
        }
        
    }

}
