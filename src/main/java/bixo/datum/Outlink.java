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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* An outgoing link from a page. */
public class Outlink implements Writable {

    private static final String NO_FOLLOW_REL_ATTRIBUTE = "nofollow";

    private String _toUrl;
    private String _anchorText;
    private String _relAttributes;

    public Outlink() {
    }

    public Outlink(String toUrl, String anchorText, String relAttributes) {
        this._toUrl = toUrl;
        if (anchorText == null)
            anchorText = "";
        _anchorText = anchorText;
        _relAttributes = relAttributes;
    }
    
    public Outlink(String toUrl, String anchorText) {
        this(toUrl, anchorText, null);
    }

    public String getToUrl() {
        return _toUrl;
    }

    public String getAnchor() {
        return _anchorText;
    }

    public String getRelAttributes() {
        return _relAttributes;
    }
    
    public boolean isNoFollow() {
        String relAttributesString = getRelAttributes();
        if (relAttributesString != null) {
            String[] relAttributes = relAttributesString.split("[, \t]");
            for (String relAttribute : relAttributes) {
                if (relAttribute.equalsIgnoreCase(NO_FOLLOW_REL_ATTRIBUTE)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @Override
	public String toString() {
        return "toUrl: " + _toUrl + " anchor: " + _anchorText; // removed "\n".
        // toString, not
        // printLine... WD.
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_anchorText == null) ? 0 : _anchorText.hashCode());
		result = prime * result + ((_toUrl == null) ? 0 : _toUrl.hashCode());
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
		if (_anchorText == null) {
			if (other._anchorText != null)
				return false;
		} else if (!_anchorText.equals(other._anchorText))
			return false;
		if (_toUrl == null) {
			if (other._toUrl != null)
				return false;
		} else if (!_toUrl.equals(other._toUrl))
			return false;
		return true;
	}
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _toUrl = in.readUTF();
        _anchorText = in.readUTF();
        _relAttributes = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_toUrl);
        out.writeUTF(_anchorText);
        out.writeUTF(_relAttributes);
    }


}
