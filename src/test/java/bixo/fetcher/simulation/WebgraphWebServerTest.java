/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.fetcher.simulation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.BitSet;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

public class WebgraphWebServerTest {

    @Test
    public void testCrawlWebGraph() throws Exception {
        if (!checkIfWebgraphExists()) {
            return;
        }
        WebgraphWebServer webServer = new WebgraphWebServer("extras/webgraph/eu-2005", 8088);
        int[] nodes = webServer.getOutlinksNodes(0);
        String[] urls = webServer.getUrlsForNodes(nodes);
        Assert.assertEquals(nodes.length, urls.length);
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i];
            int node = webServer.getNode(url);
            String url2 = webServer.getUrlforNode(node);
            Assert.assertEquals(url, url2);
        }
        webServer.stop();
    }

    @Test
    public void testGetURLs() throws Exception {
        if (!checkIfWebgraphExists()) {
            return;
        }
        WebgraphWebServer webServer = new WebgraphWebServer("extras/webgraph/eu-2005", 8088);
        URL[] urls = new URL[] { new URL("http://127.0.0.1:8088/node/0"), new URL("http://127.0.0.1:8088/url/http://www.europol.eu.int/") };

        for (URL url : urls) {

            InputStream openStream = url.openStream();

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            byte[] buf = new byte[128];
            int l = -1;
            while ((l = openStream.read(buf)) != -1) {
                stream.write(buf, 0, l);
            }
            String content = new String(stream.toByteArray());
            System.out.println(content);
        }
        webServer.stop();
    }

    @Test
    /*
     * Attention this test runs more than 10 min on my mac. We test here if
     * actually really can hit each node in the graph with a seed of 10%.
     */
    public void testGetCompleteGraph() throws Exception {
        if (!checkIfWebgraphExists()) {
            return;
        }
        // lets try if we really can access the complete graph
        WebgraphWebServer webServer = new WebgraphWebServer("extras/webgraph/eu-2005", 8088);
        LinkedList<Integer> queue = new LinkedList<Integer>();

        int size = webServer.getNumOfNodes();

        System.out.println("Pages to crawl: " + size);
        BitSet bitSet = new BitSet(size);
        // put every 10th id into the queue
        for (int i = 0; i < size; i += 10) {
            queue.push(i);
        }

        while (bitSet.cardinality() < size && queue.size() > 0) {
            if (bitSet.cardinality() % 10000 == 0) {
                System.out.println("Crawled nodes: " + bitSet.cardinality());
            }
            int nodeId = queue.poll();
            if (!bitSet.get(nodeId)) {
                // ok we saw this node
                bitSet.set(nodeId, true);
                // get out links
                int[] nodes = webServer.getOutlinksNodes(nodeId);
                for (int outNodeId : nodes) { // push links in queue but only if
                    // we never saw it before
                    if (!bitSet.get(outNodeId)) {
                        queue.push(outNodeId);
                    }
                }
            }

        }
        webServer.stop();
    }

    private boolean checkIfWebgraphExists() {
        if (!new File("extras/webgraph/eu-2005.graph").exists()) {
            System.err.println("ATTENTION This test runs only if you have the eu-2005 webgraph downloaded in extras/webgraph. See extras/webgraph for details.");
            return false;
        }
        return true;
    }

}
