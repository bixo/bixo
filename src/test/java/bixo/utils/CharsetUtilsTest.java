package bixo.utils;

import org.junit.Test;
import static org.junit.Assert.*;

public class CharsetUtilsTest {

    @Test
    public void testInvalidCharset() {
        assertFalse(CharsetUtils.safeIsSupported(" utf-8"));
        assertFalse(CharsetUtils.safeIsSupported("my charset name"));
        assertFalse(CharsetUtils.safeIsSupported("charset1; charset2"));
        assertFalse(CharsetUtils.safeIsSupported(null));
        assertFalse(CharsetUtils.safeIsSupported(""));
    }
    
    @Test
    public void testValidCharset() {
        assertTrue(CharsetUtils.safeIsSupported("UTF-8"));
        assertFalse(CharsetUtils.safeIsSupported("bogus"));
    }
    
    @Test
    public void testCleaningCharsetName() {
        assertEquals("utf-8", CharsetUtils.clean("utf-8"));
        assertEquals(null, CharsetUtils.clean(""));
        assertEquals(null, CharsetUtils.clean(null));
        assertEquals("us-ascii", CharsetUtils.clean(" us-ascii  "));
        assertEquals("utf-8", CharsetUtils.clean("\"utf-8\""));
        assertEquals("utf-8", CharsetUtils.clean("utf-8; latin-1"));
    }
}
