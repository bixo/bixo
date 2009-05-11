package bixo.fetcher.http;

import org.junit.Assert;
import org.junit.Test;


public class SimpleRobotRulesTest {
    private static final String LF = "\n";
    private static final String CR = "\r";
    
    private static final boolean[] ACCEPT_ALL = {
      true,   // "/a",          
      true,   // "/a/",         
      true,   // "/a/bloh/foo.html"
      true,   // "/b",          
      true,   // "/b/a",        
      true,   // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      true,   // "/d",          
      true,   // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      true,   // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      true,   // "/foo/bar.html",  
      true,   // "/f/",
    };
    
    private static final String[] ROBOTS_STRINGS= new String[] {
      "User-Agent: Agent1 #foo" + CR 
      + "Disallow: /a" + CR 
      + "Disallow: /b/a" + CR 
      + "#Disallow: /c" + CR 
      + "" + CR 
      + "" + CR 
      + "User-Agent: Agent2 Agent3#foo" + CR 
      + "User-Agent: Agent4" + CR 
      + "Disallow: /d" + CR 
      + "Disallow: /e/d/" + CR
      + "" + CR 
      + "User-Agent: *" + CR 
      + "Disallow: /foo/bar/" + CR,
      
      null  // Used to test EMPTY_RULES
    };

    private static final String[] AGENT_STRINGS= new String[] {
      "Agent1",
      "Agent2",
      "Agent3",
      "Agent4",
      "Agent5",
    };

    private static final boolean[][] NOT_IN_ROBOTS_STRING= new boolean[][] {
      { 
        false, 
        false,
        false,
        false,
        true,
      },
      { 
        false, 
        false,
        false,
        false,
        true,
      }    
    };

    private static final String[] TEST_PATHS= new String[] {
      "/a",
      "/a/",
      "/a/bloh/foo.html",
      "/b",
      "/b/a",
      "/b/a/index.html",
      "/b/b/foo.html",
      "/c",
      "/c/a",
      "/c/a/index.html",
      "/c/b/foo.html",
      "/d",
      "/d/a",
      "/e/a/index.html",
      "/e/d",
      "/e/d/foo.html",
      "/e/doh.html",
      "/f/index.html",
      "/foo/bar/baz.html",  
      "/f/",
    };

    private static final boolean[][][] ALLOWED= new boolean[][][] {
      { // ROBOTS_STRINGS[0]
        { // Agent1
      false,  // "/a",          
      false,  // "/a/",         
      false,  // "/a/bloh/foo.html"
      true,   // "/b",          
      false,  // "/b/a",        
      false,  // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      true,   // "/d",          
      true,   // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      true,   // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      true,   // "/foo/bar.html",  
      true,   // "/f/",  
        }, 
        { // Agent2
      true,   // "/a",          
      true,   // "/a/",         
      true,   // "/a/bloh/foo.html"
      true,   // "/b",          
      true,   // "/b/a",        
      true,   // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      false,  // "/d",          
      false,  // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      false,  // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      true,   // "/foo/bar.html",  
      true,   // "/f/",  
        },
        { // Agent3
      true,   // "/a",          
      true,   // "/a/",         
      true,   // "/a/bloh/foo.html"
      true,   // "/b",          
      true,   // "/b/a",        
      true,   // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      false,  // "/d",          
      false,  // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      false,  // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      true,   // "/foo/bar.html",  
      true,   // "/f/",  
        },
        { // Agent4
      true,   // "/a",          
      true,   // "/a/",         
      true,   // "/a/bloh/foo.html"
      true,   // "/b",          
      true,   // "/b/a",        
      true,   // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      false,  // "/d",          
      false,  // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      false,  // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      true,   // "/foo/bar.html",  
      true,   // "/f/",  
        },
        { // Agent5/"*"
      true,   // "/a",          
      true,   // "/a/",         
      true,   // "/a/bloh/foo.html"
      true,   // "/b",          
      true,   // "/b/a",        
      true,   // "/b/a/index.html",
      true,   // "/b/b/foo.html",  
      true,   // "/c",          
      true,   // "/c/a",        
      true,   // "/c/a/index.html",
      true,   // "/c/b/foo.html",  
      true,   // "/d",          
      true,   // "/d/a",        
      true,   // "/e/a/index.html",
      true,   // "/e/d",        
      true,   // "/e/d/foo.html",  
      true,   // "/e/doh.html",    
      true,   // "/f/index.html",  
      false,  // "/foo/bar.html",  
      true,   // "/f/",  
        }
      },
      { // ROBOTS_STRINGS[1]
        ACCEPT_ALL, // Agent 1
        ACCEPT_ALL, // Agent 2
        ACCEPT_ALL, // Agent 3
        ACCEPT_ALL, // Agent 4
        ACCEPT_ALL, // Agent 5
      }
    };

    @Test
    public void emptyTest() {
        // To keep JUnit from complaining, until the rest of the tests in here are
        // re-enabled.
    }
    
    public void testRobotsOneAgent() {
        for (int i= 0; i < ROBOTS_STRINGS.length; i++) {
            for (int j= 0; j < AGENT_STRINGS.length; j++) {
                testRobots(i, AGENT_STRINGS[j], TEST_PATHS, ALLOWED[i][j]);
            }
        }
    }

//    @Test
//    public void testRobotsTwoAgents() {
//      for (int i= 0; i < ROBOTS_STRINGS.length; i++) {
//        for (int j= 0; j < AGENT_STRINGS.length; j++) {
//      for (int k= 0; k < AGENT_STRINGS.length; k++) {
//        int key= j;
//        if (NOT_IN_ROBOTS_STRING[i][j])
//          key= k;
//        testRobots(i, new String[] { AGENT_STRINGS[j], AGENT_STRINGS[k] },
//               TEST_PATHS, ALLOWED[i][key]);
//      }
//        }
//      }
//    }
//    
    public void testCrawlDelay() {
      String delayRule1 = "User-agent: nutchbot" + CR +
                          "Crawl-delay: 10" + CR +
                          "User-agent: foobot" + CR +
                          "Crawl-delay: 20" + CR +
                          "User-agent: *" + CR + 
                          "Disallow:/baz" + CR;
      SimpleRobotRules rules = new SimpleRobotRules("nutchbot", delayRule1.getBytes());
      int crawlDelay = rules.getCrawlDelay();
      Assert.assertEquals("testing crawl delay for agent nutchbot - rule 1", 10000, crawlDelay);
      
      String delayRule2 = "User-agent: foobot" + CR +
      "Crawl-delay: 20" + CR +
      "User-agent: *" + CR + 
      "Disallow:/baz" + CR;
      rules = new SimpleRobotRules("nutchbot", delayRule2.getBytes());
      crawlDelay = rules.getCrawlDelay();
      Assert.assertEquals("testing crawl delay for agent nutchbot - rule 2", SimpleRobotRules.NO_CRAWL_DELAY, crawlDelay);
    }

    // helper

    private void testRobots(int robotsString, String agent, String[] paths, boolean[] allowed) {
        SimpleRobotRules rules = new SimpleRobotRules(agent, (ROBOTS_STRINGS[robotsString] != null
                        ? ROBOTS_STRINGS[robotsString].getBytes()
                                        : null));
        for (int i= 0; i < paths.length; i++) {
            String msg =  "testing robots file " + robotsString + ", on agent "
                + agent + " and path " + TEST_PATHS[i] + "; got " 
                + rules.isAllowed(TEST_PATHS[i]) + ", rules are: " + LF
                + rules;
            
            Assert.assertEquals(msg, allowed[i], rules.isAllowed(TEST_PATHS[i]));
        }
    }

}
