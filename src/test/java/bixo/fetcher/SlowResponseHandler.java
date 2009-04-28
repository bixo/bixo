package bixo.fetcher;

import java.io.IOException;
import java.io.OutputStream;

import org.mortbay.http.HttpException;
import org.mortbay.http.HttpRequest;
import org.mortbay.http.HttpResponse;
import org.mortbay.http.handler.AbstractHttpHandler;

@SuppressWarnings("serial")
public class SlowResponseHandler extends AbstractHttpHandler {
    private int _length;
    private long _duration;
    
    /**
     * Create an HTTP response handler that trickles back data.
     * 
     * @param length - bytes to return
     * @param duration - duration for response, in milliseconds.
     */
    public SlowResponseHandler(int length, long duration) {
        _length = length;
        _duration = duration;
    }
    
    @Override
    public void handle(String pathInContext, String pathParams, HttpRequest request, HttpResponse response) throws HttpException, IOException {
        response.setContentLength(_length);
        response.setContentType("text/html");
        response.setStatus(200);
        
        OutputStream os = response.getOutputStream();
        long startTime = System.currentTimeMillis();
        
        try {
            for (long i = 0; i < _length; i++) {
                os.write(0);
                
                // Given i/_length as % of data written, we know that
                // this * duration is the target elapsed time. Figure out
                // how much to delay to make it so.
                long curTime = System.currentTimeMillis();
                long elapsedTime = curTime - startTime;
                long targetTime = (i * _duration) / (long)_length;
                if (elapsedTime < targetTime) {
                    Thread.sleep(targetTime - elapsedTime);
                }
            }
        } catch (InterruptedException e) {
            throw new HttpException(500, "Response handler interrupted");
        }
    }
}
