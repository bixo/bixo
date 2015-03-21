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
package bixo.examples.webmining;

import com.scaleunlimited.cascading.BaseDatum;



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
