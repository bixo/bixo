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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;



/**
 * A webserver serving content out of a webgraph.
 * 
 * @see <a href="http://webgraph.dsi.unimi.it/">WebGraph framework</a>
 * 
 */
public class WebgraphWebServer extends SimulationWebServer {

    private List<CharSequence> _node2url;
    private Object2LongFunction<? extends CharSequence> _url2node;
    private ImmutableGraph _graph;
    private int _port;
    private Server _server;

    /**
     * @param pathToWebgraphBase
     *            e.g. /webgraph/eu-2005 where there is a u-2005.fcl eu-2005.
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public WebgraphWebServer(String pathToWebgraphBase, int port) throws Exception {
        _port = port;
        ProgressLogger pl = new ProgressLogger();

        String fcl = pathToWebgraphBase + ".fcl";
        _node2url = (List<CharSequence>) BinIO.loadObject(fcl);

        String mph = pathToWebgraphBase + ".mph";
        _url2node = (Object2LongFunction<? extends CharSequence>) BinIO.loadObject(mph);

        _graph = ImmutableGraph.load(pathToWebgraphBase, pl);
        _server = startServer(new WebgraphHandler(), port);

    }

    private class WebgraphHandler extends AbstractHandler {

        private static final String NODE = "/node/";
        private static final String URL = "/url/";

        @Override
        public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
            byte[] bytes = null;
            if (pathInContext.startsWith(NODE)) {
                String nodeIdStr = pathInContext.substring(NODE.length());
                int nodeId = Integer.parseInt(nodeIdStr);
                int[] outlinksNodes = getOutlinksNodes(nodeId);
                bytes = renderResponse(outlinksNodes);

            } else if (pathInContext.startsWith(URL)) {
                String reqUrl = pathInContext.substring(URL.length());
                String[] urlsForNodes = getUrlsForNodes(getOutlinksNodes(getNode(reqUrl)));
                bytes = renderResponse(urlsForNodes);
            } else {
                throw new RuntimeException("unhandleable url");
            }
            if (response instanceof  Response) {
                Response jettyResponse = (Response) response;
                jettyResponse.setHeader("Content-Length", "" + bytes.length);
                jettyResponse.getOutputStream().write(bytes);
                jettyResponse.getOutputStream().flush();
            }

        }

        private byte[] renderResponse(String[] urlsForNodes) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("<html><head/><body>");
            for (String url : urlsForNodes) {
                buffer.append("<a href=\"http://127.0.0.1:" + _port + "/url/" + url + "\"> " + url + " link </a> ");
            }
            buffer.append("</body><html>");
            return buffer.toString().getBytes();
        }

        private byte[] renderResponse(int[] outlinksNodes) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("<html><head/><body>");
            for (int nodeId : outlinksNodes) {
                buffer.append("<a href=\"http://127.0.0.1:" + _port + "/node/" + nodeId + "\"> " + nodeId + " link </a> ");
            }
            buffer.append("</body><html>");
            return buffer.toString().getBytes();
        }
    }

    protected int[] getOutlinksNodes(int nodeId) {
        return _graph.successorArray(nodeId);
    }

    protected String[] getUrlsForNodes(int[] nodes) {
        String[] urls = new String[nodes.length];
        for (int i = 0; i < urls.length; i++) {
            urls[i] = getUrlforNode(i);
        }
        return urls;
    }

    protected int getNode(String url) {
        // TODO SG that is everything else than clean
        return (int) _url2node.get(url).longValue();
    }

    protected String getUrlforNode(int node) {
        return _node2url.get(node).toString();
    }

    protected int getNumOfNodes() {
        return _graph.numNodes();

    }

    public void stop() throws Exception {
        _server.stop();
    }
}
