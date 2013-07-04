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

public class MultiValuePreference implements Writable {

    public static final String USER_ID_FN = "userid";
    public static final String ACTION_ID_FN = "actionid";
    public static final String ITEM_ID_FN = "actionid";

    private String _userId;
    private String _actionId;
    private String _itemId;

    public MultiValuePreference() {
        // Empty constructor for deserialization
    }

    public MultiValuePreference(String userId, String actionId, String itemId) {
        _userId = userId;
        _actionId = actionId;
        _itemId = itemId;
    }

    public String getUserId() {
        return _userId;
    }

    public void setUserId(String userId) {
        _userId = userId;
    }

    public String getActionId() {
        return _actionId;
    }

    public void setActionId(String actionId) {
        _actionId = actionId;
    }


    public String getItemId() {

        return _itemId;
    }

    public void setItemId(String itemId) {
        _itemId = itemId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _userId = in.readUTF();
        _actionId = in.readUTF();
        _itemId = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_userId);
        out.writeUTF(_actionId);
        out.writeUTF(_itemId);
    }
    
}
