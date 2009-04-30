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

    private HashMap<String, Comparable> _metaDataMap;

    public BaseDatum(Map<String, Comparable> metaData) {
        _metaDataMap = (HashMap<String, Comparable>) metaData;
    }

    public BaseDatum(Tuple tuple, Fields metaDataFields) {
        ArrayList<Comparable> fieldNames = new ArrayList<Comparable>();
        Iterator iterator = metaDataFields.iterator();

        while (iterator.hasNext()) {
            String fieldName = (String) iterator.next();
            fieldNames.add(fieldName);
        }

        Collections.sort(fieldNames);
        _metaDataMap = new HashMap<String, Comparable>();
        
        int startingOffset = getStandardFields().size();
        for (int i = 0; i < fieldNames.size(); i++) {
            String key = (String) fieldNames.get(i);
            Comparable value = tuple.get(startingOffset + i);
            _metaDataMap.put(key, value);
        }

    }
    
    protected abstract Comparable[] getStandardValues();
    protected abstract Fields getStandardFields();

    public Fields getMetaDataFields() {
        Fields result = new Fields();
        if (_metaDataMap != null) {
            String[] keys = _metaDataMap.keySet().toArray(new String[_metaDataMap.size()]);
            Arrays.sort(keys);
            for (String key : keys) {
                result.append(new Fields(key));
            }
        }
        
        return result;
    }
    
    public Comparable[] getMetaDataValues() {
        Fields metaDataFields = getMetaDataFields();
        Comparable[] result = new Comparable[metaDataFields.size()];
        Iterator iterator = metaDataFields.iterator();

        int i = 0;
        while (iterator.hasNext()) {
            String fieldName = (String)iterator.next();
            result[i++] = getMetaDataValue(fieldName);
        }
        
        return result;
    }
    
    public Comparable getMetaDataValue(String key) {
        if ((_metaDataMap == null) || (!_metaDataMap.containsKey(key))) {
            throw new IllegalStateException("No meta-data field named " + key);
        } else {
            return _metaDataMap.get(key);
        }
    }

    public void setMetaDataValue(String key, Comparable value) {
        if ((_metaDataMap == null) || (!_metaDataMap.containsKey(key))) {
            throw new IllegalStateException("No meta-data field named " + key);
        }

        _metaDataMap.put(key, value);
    }

    public Map<String, Comparable> getMetaDataMap() {
        return _metaDataMap;
    }

    

    /**
     * Create a tuple from the "standard" fields, plus all of the meta-data,
     * where the order of the meta-data values is based on String sort order for
     * the field names (map key values).
     * 
     * @return new Tuple
     */
    public Tuple toTuple() {
        Tuple tuple = new Tuple(getStandardValues());
        
        Comparable[] metaDataValues = getMetaDataValues();
        for (Comparable value : metaDataValues) {
            tuple.add(value);
        }

        return tuple;
    }

    protected static String fieldName(Class clazz, String field) {
        return clazz.getSimpleName() + "-" + field;
    }
    
    
}
