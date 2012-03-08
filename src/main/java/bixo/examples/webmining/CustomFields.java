/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import com.bixolabs.cascading.BaseDatum;


public class CustomFields {

    public static final String PAGE_SCORE_FN =
        BaseDatum.fieldName(CustomFields.class, "pagescore");

    public static final String LINKS_SCORE_FN =
        BaseDatum.fieldName(CustomFields.class, "linksscore");

    public static final String STATUS_FN =
        BaseDatum.fieldName(CustomFields.class, "status");
    
    public static final String SKIP_BY_LIMIT_FN = 
        BaseDatum.fieldName(CustomFields.class, "limited");

}
