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

import java.io.Serializable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class GroupedUrlDatum extends UrlDatum implements Serializable, Comparable<GroupedUrlDatum> {

    private static final String GROUP_KEY_FN = fieldName(GroupedUrlDatum.class, "groupKey");
    public static final Fields FIELDS = new Fields(GROUP_KEY_FN).append(getSuperFields(GroupedUrlDatum.class));
    
    public GroupedUrlDatum() {
        super(FIELDS);
    }
    
    public GroupedUrlDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }
    
    public GroupedUrlDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public GroupedUrlDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public GroupedUrlDatum(String url, String groupKey) {
        super(FIELDS, url);
        setGroupKey(groupKey);
    }

    public GroupedUrlDatum(Fields fields, String url, String groupKey) {
        super(fields, url);
        setGroupKey(groupKey);
    }

    public GroupedUrlDatum(UrlDatum datum, String groupKey) {
        super(FIELDS, datum.getUrl());
        setGroupKey(groupKey);
        setPayload(datum);
    }
    
    public String getGroupKey() {
        return _tupleEntry.getString(GROUP_KEY_FN);
    }

    public void setGroupKey(String groupKey) {
        _tupleEntry.setString(GROUP_KEY_FN, groupKey);
    }
    
    public static Fields getGroupingField() {
        return new Fields(GROUP_KEY_FN);
    }

    @Override
    public int compareTo(GroupedUrlDatum o) {
        // We don't care how these get ordered in the DiskQueue
        return 0;
    }
}
