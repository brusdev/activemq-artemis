/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.json;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class JsonObject extends JsonValue {

   private final javax.json.JsonObject rawObject;

   public javax.json.JsonObject getRawObject() {
      return rawObject;
   }

   public JsonObject(javax.json.JsonObject rawObject) {
      super(rawObject);
      this.rawObject = rawObject;
   }

   public JsonArray getJsonArray(String name) {
      return new JsonArray(rawObject.getJsonArray(name));
   }

   public JsonObject getJsonObject(String name) {
      return new JsonObject(rawObject.getJsonObject(name));
   }

   public JsonNumber getJsonNumber(String name) {
      return new JsonNumber(rawObject.getJsonNumber(name));
   }

   public JsonString getJsonString(String name) {
      return new JsonString(rawObject.getJsonString(name));
   }

   public String getString(String name) {
      return rawObject.getString(name);
   }

   public String getString(String name, String defaultValue) {
      return rawObject.getString(name, defaultValue);
   }

   public int getInt(String name) {
      return rawObject.getInt(name);
   }

   public int getInt(String name, int defaultValue) {
      return rawObject.getInt(name, defaultValue);
   }

   public boolean getBoolean(String name) {
      return rawObject.getBoolean(name);
   }

   public boolean getBoolean(String name, boolean defaultValue) {
      return rawObject.getBoolean(name, defaultValue);
   }

   public boolean isNull(String name) {
      return rawObject.isNull(name);
   }

   public int size() {
      return rawObject.size();
   }

   public JsonValue get(String name) {
      return JsonValue.wrap(rawObject.get(name));
   }

   public boolean containsKey(String key) {
      return rawObject.containsKey(key);
   }

   public Set<String> keySet() {
      return rawObject.keySet();
   }

   public Collection<JsonValue> values() {
      return new AbstractCollection<JsonValue>() {
         @Override
         public Iterator<JsonValue> iterator() {
            return new Iterator<JsonValue>() {
               private Iterator<javax.json.JsonValue> rawIterator = rawObject.values().iterator();

               @Override
               public boolean hasNext() {
                  return rawIterator.hasNext();
               }

               @Override
               public JsonValue next() {
                  return JsonValue.wrap(rawIterator.next());
               }
            };
         }

         @Override
         public int size() {
            return rawObject.size();
         }
      };
   }

   public Set<Map.Entry<String, JsonValue>> entrySet() {
      return new AbstractSet<Map.Entry<String, JsonValue>>() {
         @Override
         public Iterator<Map.Entry<String, JsonValue>> iterator() {
            return new Iterator<Map.Entry<String, JsonValue>>() {
               private Iterator<Map.Entry<String, javax.json.JsonValue>> rawIterator = rawObject.entrySet().iterator();

               @Override
               public boolean hasNext() {
                  return rawIterator.hasNext();
               }

               @Override
               public Map.Entry<String, JsonValue> next() {
                  Map.Entry<String, javax.json.JsonValue> rawEntry = rawIterator.next();

                  return rawEntry != null ? new AbstractMap.SimpleEntry<>(rawEntry.getKey(), JsonValue.wrap(rawEntry.getValue())) : null;
               }
            };
         }

         @Override
         public int size() {
            return rawObject.size();
         }
      };
   }

   public boolean isEmpty() {
      return rawObject.isEmpty();
   }
}
