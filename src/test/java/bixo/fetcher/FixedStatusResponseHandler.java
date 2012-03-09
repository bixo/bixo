package bixo.fetcher;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.handler.AbstractHandler;


/**
 * Reponse handler that always returns a 404.
 *
 */
public class FixedStatusResponseHandler extends AbstractHandler {
    private int _status;
    
    public FixedStatusResponseHandler(int status) {
        _status = status;
    }
    
    @Override
    public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
        throw new HttpException(_status, "Pre-defined error fetching: " + pathInContext);
    }
}
