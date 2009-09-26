/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bixo.datum;

/* An outgoing link from a page. */
public class Outlink {

    private String toUrl;
    private String anchor;

    public Outlink() {
    }

    public Outlink(String toUrl, String anchor) {
        this.toUrl = toUrl;
        if (anchor == null)
            anchor = "";
        this.anchor = anchor;
    }

    public String getToUrl() {
        return toUrl;
    }

    public String getAnchor() {
        return anchor;
    }

    public String toString() {
        return "toUrl: " + toUrl + " anchor: " + anchor; // removed "\n".
        // toString, not
        // printLine... WD.
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((anchor == null) ? 0 : anchor.hashCode());
		result = prime * result + ((toUrl == null) ? 0 : toUrl.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Outlink other = (Outlink) obj;
		if (anchor == null) {
			if (other.anchor != null)
				return false;
		} else if (!anchor.equals(other.anchor))
			return false;
		if (toUrl == null) {
			if (other.toUrl != null)
				return false;
		} else if (!toUrl.equals(other.toUrl))
			return false;
		return true;
	}
    
    

}
