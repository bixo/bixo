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
package com.finderbots.miner2.tomatoes;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/* This class encapsulates a mining result and writes it.
 */

public class RTCriticPreference implements Writable {

    public static final String USER_ID_FN = "user_id";
    public static final String ITEM_ID_FN = "item_id";
    public static final String USER_NAME_FN = "user_name";
    public static final String ITEM_NAME_FN = "item_name";
    public static final String PREFERENCE_VALUE_FN = "preference_value";
    //public static final String DESC_FN = "description";

    private String _userId;
    private String _itemId;
    private String _userName;
    private String _itemName;
    private String _preferenceValue;
    //private String _description;

    public RTCriticPreference() {
        // Empty constructor for deserialization
    }

    public RTCriticPreference(String userId, String itemId, String userName, String itemName, String preferenceValue) {
        _userId = userId;
        _itemId = itemId;
        _userName = userName;
        _itemName = itemName;
        _preferenceValue = preferenceValue;
    }

    public String get_userId() {
        return _userId;
    }

    public String get_itemId() {
        return _itemId;
    }

    public String get_userName() {
        return _userName;
    }

    public String get_itemName() {
        return _itemName;
    }

    public String get_preferenceValue() {
        return _preferenceValue;
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
        _userId = in.readUTF();
        _itemId = in.readUTF();
        _userName = in.readUTF();
        _itemName = in.readUTF();
        _preferenceValue = in.readUTF();
        //_description = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_userId);
        out.writeUTF(_itemId);
        out.writeUTF(_userName);
        out.writeUTF(_itemName);
        out.writeUTF(_preferenceValue);
    }

}
