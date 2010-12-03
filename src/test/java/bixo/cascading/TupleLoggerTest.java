package bixo.cascading;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import cascading.tuple.Tuple;
import static org.junit.Assert.*;

public class TupleLoggerTest {

    @Test
    public void testLimitStringLength() {
        assertEquals("abc", TupleLogger.printObject("abcdefg", 3));
    }
    
    @Test
    public void testLimitBytesWritable() {
        BytesWritable bw = new BytesWritable("0123".getBytes());
        
        assertEquals("30 31 32", TupleLogger.printObject(bw, 10));
    }
    
    @Test
    public void testRemovingCRLF() {
        assertEquals("ab cd", TupleLogger.printObject("ab\rcd", 10));
    }
    
    @Test
    public void testEmptyBytesWritable() {
        assertEquals("", TupleLogger.printObject(new BytesWritable(), 10));
    }
    
    @Test
    public void testPrintingTupleInTuple() {
        TupleLogger tl = new TupleLogger(true);
        tl.setMaxPrintLength(10);
        
        BytesWritable bw = new BytesWritable("0123456789".getBytes());

        Tuple tuple = new Tuple("A long string", 1000, bw, new Tuple("a", "b"));
        StringBuilder result = new StringBuilder();
        result = tl.printTuple(result, tuple);
        
        assertEquals("['A long str', '1000', '30 31 32', ['a', 'b']]", result.toString());
    }
    
    @Test
    public void testPrintingNull() {
        TupleLogger tl = new TupleLogger(true);
        Tuple tuple = new Tuple("a", null);
        StringBuilder result = new StringBuilder();
        result = tl.printTuple(result, tuple);
        
        assertEquals("['a', 'null']", result.toString());
    }
}
