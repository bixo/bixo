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
