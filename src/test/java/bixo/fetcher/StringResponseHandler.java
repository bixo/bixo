package bixo.fetcher;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.handler.AbstractHandler;


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
    public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
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
