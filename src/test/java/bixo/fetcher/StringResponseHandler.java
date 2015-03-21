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
package bixo.fetcher;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;



public class StringResponseHandler extends AbstractHandler {
    
    private String _contentType;
    private String _response;
    
    /**
     * Create an HTTP response handler that always sends back a fixed string
     * 
     */
    public StringResponseHandler(String contentType, String response) {
        _contentType = contentType;
        _response = response;
    }
    
    @Override
    public void handle(String pathInContext, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws HttpException, IOException {
        try {
            byte[] bytes = _response.getBytes("UTF-8");
            response.setContentLength(bytes.length);
            response.setContentType(_contentType);
            response.setStatus(200);
            
            OutputStream os = response.getOutputStream();
            os.write(bytes);
        } catch (Exception e) {
            throw new HttpException(500, e.getMessage());
        }
    }
}
