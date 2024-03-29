/*
 * Copyright 2022 ICCS
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
 */

package gr.ntua.ece.cslab.datasource.bda.datastore.beans;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * This class represents a key value pair, used by the {@link Message} class.
 */
@XmlRootElement(name = "KeyValue")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class KeyValue implements Serializable{
    private String key;
    private String value;

    /**
     * Default constructor
     */
    public KeyValue() {}

    /**
     * Default constructor that initializes the key and the value.
     * @param key
     * @param value
     */
    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Getter for the key
     * @return
     */
    public String getKey() {
        return key;
    }

    /**
     * Setter for the key
     * @param key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Getter for the value
     * @return
     */
    public String getValue() {
        return value;
    }

    /**
     * Setter for the value
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }

    protected Object castValue(JSONObject info) {
        if (info.getString(key).equals("int") || info.getString(key).equals("integer")) {
            return Integer.valueOf(value);
        } else if (info.getString(key).equals("double")) {
            return Double.valueOf(value);
        } else {
            return value;
        }
    }
}
