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
package bixo.datum;


import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class OutlinkTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testIsNoFollow() throws Exception {
        Assert.assertTrue(makeOutlink("nofollow").isNoFollow());
        Assert.assertTrue(makeOutlink("other1,noFollow other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1 noFollow,other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1,noFollow\tother2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1\tnoFollow,other2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1  noFollow,\t\tother2").isNoFollow());
        Assert.assertTrue(makeOutlink("other1=other1value\t\t  ,nofollow other2").isNoFollow());
        Assert.assertFalse(makeOutlink("nofollowing").isNoFollow());
    }

    private Outlink makeOutlink(String relAttributes) {
        return new Outlink( "http://www.test-domain.com/target.html",
                            "Interesting target",
                            relAttributes);
    }
}
