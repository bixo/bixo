package bixo.urls;

import bixo.datum.UrlDatum;

/**
 * Simple UrlFilter that uses a URL validator.
 */
@SuppressWarnings("serial")
public class SimpleUrlFilter extends BaseUrlFilter {

    private BaseUrlValidator _validator;
    
    public SimpleUrlFilter() {
        this(new SimpleUrlValidator());
    }
    
    public SimpleUrlFilter(BaseUrlValidator validator) {
        _validator = validator;
    }
    
	@Override
	public boolean isRemove(UrlDatum datum) {
	    return !_validator.isValid(datum.getUrl());
	}

}
