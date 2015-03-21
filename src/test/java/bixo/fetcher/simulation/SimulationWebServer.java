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

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;



public abstract class SimulationWebServer {

    public Server startServer(Handler handler, int port) throws Exception {
        Server server = new Server(port);
        server.setHandler(handler);
        server.start();
        return server;
    }

}
