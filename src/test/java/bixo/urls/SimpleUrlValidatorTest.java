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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import bixo.urls.SimpleUrlValidator;

public class SimpleUrlValidatorTest {

    private SimpleUrlValidator _validator;
    @Before
    public void setup() {
       _validator = new SimpleUrlValidator();
    }
    
    @Test
    public void testValidate() {
        Assert.assertEquals(true, _validator.isValid("http://foo.com"));
        Assert.assertEquals(true, _validator.isValid("http://www.foo.com"));
        Assert.assertEquals(true, _validator.isValid("http://www.foo.com/"));
        Assert.assertEquals(true, _validator.isValid("http://aws.foo.com/"));
        Assert.assertEquals(true, _validator.isValid("https://aws.foo.com/"));

        Assert.assertEquals(false, _validator.isValid("foo.com"));
        Assert.assertEquals(false, _validator.isValid("www.foo.com"));
        Assert.assertEquals(false, _validator.isValid("mailto://ken@foo.com"));
        Assert.assertEquals(false, _validator.isValid("mailto:?Subject=http://info.foo.com/copyright/us/details.html"));
        Assert.assertEquals(false, _validator.isValid("smtp://aws.foo.com/"));
        Assert.assertEquals(false, _validator.isValid("ftp://aws.foo.com/"));
        Assert.assertEquals(false, _validator.isValid("javascript:foobar()"));
        
        Assert.assertEquals(false, _validator.isValid("feed://getbetterhealth.com/feed"));
        Assert.assertEquals(false, _validator.isValid("ttp://www.thehealthcareblog.com/the_health_care_blog/2009/07/healthcare-reform-lessons-from-mayo-clinic.html"));
   }
    
}
