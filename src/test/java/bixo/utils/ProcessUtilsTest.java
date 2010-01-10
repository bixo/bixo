package bixo.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.log4j.Logger;
import org.junit.Test;

public class ProcessUtilsTest {
    private static final Logger LOGGER = Logger.getLogger(ProcessUtilsTest.class);
    
    @Test
    public void testLsofCmd() {
        final String[] commandLine = new String[] { "lsof" };
        StringBuffer infoBuffer = new StringBuffer();
        StringBuffer errorBuffer = new StringBuffer();
        
        try {
            assertEquals(0, ProcessUtils.execute(commandLine, infoBuffer, errorBuffer, null));
            String[] lines = infoBuffer.toString().split("\n");
            for (String line : lines) {
                LOGGER.info(line);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }
}
