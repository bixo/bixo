package bixo.fetcher;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.mortbay.jetty.HttpException;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * Reponse handler that always returns a 404.
 *
 */
public class RedirectResponseHandler extends AbstractHandler {
    
    private String _originalPath;
    private String _redirectUrl;
    
    public RedirectResponseHandler(String originalPath, String redirectUrl) {
        _originalPath = originalPath;
        _redirectUrl = redirectUrl;
    }
    
    @Override
    public void handle(String pathInContext, HttpServletRequest request, HttpServletResponse response, int dispatch) throws HttpException, IOException {
        if (pathInContext.equalsIgnoreCase(_originalPath)) {
            response.sendRedirect(_redirectUrl);
        } else if (_redirectUrl.contains(pathInContext)) {
            response.setStatus(HttpStatus.SC_OK);
            response.setContentType("text/plain");

            String content = "redirected content";
            response.setContentLength(content.length());
            response.getOutputStream().write(content.getBytes());
        } else {
            response.setStatus(HttpStatus.SC_OK);
            response.setContentType("text/plain");

            String content = "other content";
            response.setContentLength(content.length());
            response.getOutputStream().write(content.getBytes());
        }
    }
}
