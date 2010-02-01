package bixo.cascading;

import java.io.Serializable;
import java.security.InvalidParameterException;

/**
 * Type that can be used as the grouping field value in a Tuple, where
 * the range of values and hash codes should be exactly equal to the
 * number of reducers, to ensure one group and one group only gets passed
 * to each of the reducers.
 * 
 * It's up to the Flow constructor to ensure that there are exactly the
 * same number of reduce tasks as there are reducers. If there are more
 * reducer tasks than reducers, you can still wind up with multiple
 * lists getting passed to the same reducer, as two different tasks.
 *
 */
@SuppressWarnings({ "serial", "unchecked" })
public class PartitioningKey implements Serializable, Comparable {
    private int _value;
    private String _ref;
    
    public PartitioningKey(String stringForm) {
        int index = stringForm.lastIndexOf('-');
        if (index == -1) {
            throw new InvalidParameterException("Invalid string form for PartitioningKey: " + stringForm);
        }
        
        _ref = stringForm.substring(0, index);
        _value = Integer.parseInt(stringForm.substring(index + 1));
    }
    
    public PartitioningKey(String ref, int numReducerTasks) {
        _ref = ref;
        _value = (ref.hashCode() & Integer.MAX_VALUE) % numReducerTasks;
    }
    
    public String getRef() {
        return _ref;
    }
    
    @Override
    public String toString() {
        return _ref + "-" + _value;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     * 
     * Ensure that there's a one-to-one mapping between values and reduce tasks,
     * since the Cascading GroupingPartitioner class uses the hash value to
     * return a partition index.
     */
    @Override
    public int hashCode() {
        return _value;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (getClass() != obj.getClass())
            return false;
        PartitioningKey other = (PartitioningKey) obj;
        return _value == other._value;
    }
    
    @Override
    public int compareTo(Object o) {
        int otherValue = ((PartitioningKey)o)._value;
        
        if (_value < otherValue) {
            return -1;
        } else if (_value > otherValue) {
            return 1;
        } else {
            return 0;
        }
    }

}
