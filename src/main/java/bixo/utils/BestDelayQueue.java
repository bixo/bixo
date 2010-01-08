package bixo.utils;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class BestDelayQueue<E extends Delayed> extends DelayQueue<E> {
    
    private static final int DEFAULT_ELEMENTS_TO_INSPECT = 20;
    
    private int _maxElementToInspect;
    
    public BestDelayQueue() {
        this(DEFAULT_ELEMENTS_TO_INSPECT);
    }
    
    public BestDelayQueue(int maxElementsToInspect) {
        if (maxElementsToInspect < 1) {
            throw new InvalidParameterException("maxElementsToInspect must be >= 1");
        }
        
        _maxElementToInspect = maxElementsToInspect;
    }
    
    
    /* (non-Javadoc)
     * @see java.util.concurrent.DelayQueue#poll()
     * 
     * Return the "best" of the pending elements, since the best one is
     * the element with the min value, which (in our case) is based on
     * the number of URLs in the element.
     */
    @Override
    public E poll() {
        List<E> available = new ArrayList<E>();
        
        E bestElement = null;
        E curElement;
        
        // Nobody else can be using the queue while we do this operation, as otherwise it's not thread-safe.
        synchronized(this) {
            while ((available.size() < _maxElementToInspect) && ((curElement = super.poll()) != null)) {
                available.add(curElement);

                if ((bestElement == null) || (curElement.getDelay(TimeUnit.MILLISECONDS) < bestElement.getDelay(TimeUnit.MILLISECONDS))) {
                    bestElement = curElement;
                }
            }

            // Now put back all of the elements that we're not returning.
            for (E element : available) {
                if (element != bestElement) {
                    if (!this.offer(element)) {
                        throw new RuntimeException("Unexpected problem pushing elements back onto queue");
                    }
                }
            }
        }
        
        return bestElement;
    }
    
}
