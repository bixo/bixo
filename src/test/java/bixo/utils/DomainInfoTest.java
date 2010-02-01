package bixo.utils;

import java.net.URISyntaxException;

import static org.junit.Assert.*;
import org.junit.Test;


public class DomainInfoTest {

    @Test
    public void testFunkyHostname() throws Exception {
        try {
            new DomainInfo("http://-subdomain.domain.com");
            fail("Should throw exception");
        } catch (URISyntaxException e) {
            // Valid.
        }
    }
    
    @Test
    public void doNotResolveTestDomain() throws Exception {
        String domain = DomainInfo.makeTestDomain(0);
        DomainInfo di = new DomainInfo("http://" + domain);
        assertEquals(di.getDomain(), di.getHostAddress());
    }
}
