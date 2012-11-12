/*
 * Copyright 2009-2012 Scale Unlimited
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
package com.finderbots.miner2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/* This class encapsulates a mining result and writes it.
 */

public class BooleanPreference implements Writable {

    public static final String TEXT_PERSON_ID_FN = "textpersonid";
    public static final String PREFERENCE_ID_FN = "preferenceid";
    //public static final String DESC_FN = "description";

    private String _personId;
    private String _preferenceId;
    //private String _description;

    public BooleanPreference() {
        // Empty constructor for deserialization
    }

    public BooleanPreference(String personId, String preferenceId) {
        _personId = personId;
        _preferenceId = preferenceId;
        //_description = description;
    }

    public String getPersonId() {
        return _personId;
    }

    public void setPersonId(String personId) {
        _personId = personId;
    }

    public String getPreferenceId() {
        return _preferenceId;
    }

    public void setPreferenceId(String preferenceId) {
        _preferenceId = preferenceId;
    }

    /*
    public String getDescription() {

        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }
    */

    @Override
    public void readFields(DataInput in) throws IOException {
        _personId = in.readUTF();
        _preferenceId = in.readUTF();
        //_description = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_personId);
        out.writeUTF(_preferenceId);
        //out.writeUTF(_description);
    }
    
}
