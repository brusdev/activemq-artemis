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

import java.util.Iterator;

public class JsonArray extends JsonValue implements Iterable<JsonValue> {

   private final javax.json.JsonArray rawArray;

   public javax.json.JsonArray getRawArray() {
      return rawArray;
   }

   public JsonArray(javax.json.JsonArray rawArray) {
      super(rawArray);
      this.rawArray = rawArray;
   }

   public JsonObject getJsonObject(int index) {
      return new JsonObject(rawArray.getJsonObject(index));
   }

   public JsonArray getJsonArray(int index) {
      return new JsonArray(rawArray.getJsonArray(index));
   }

   public JsonNumber getJsonNumber(int index) {
      return new JsonNumber(rawArray.getJsonNumber(index));
   }

   public JsonString getJsonString(int index) {
      return new JsonString(rawArray.getJsonString(index));
   }

   public String getString(int index) {
      return rawArray.getString(index);
   }

   public String getString(int index, String defaultValue) {
      return rawArray.getString(index, defaultValue);
   }

   public int getInt(int index) {
      return rawArray.getInt(index);
   }

   public int getInt(int index, int defaultValue) {
      return rawArray.getInt(index, defaultValue);
   }

   public boolean getBoolean(int index) {
      return rawArray.getBoolean(index);
   }

   public boolean getBoolean(int index, boolean defaultValue) {
      return rawArray.getBoolean(index, defaultValue);
   }

   public boolean isNull(int index) {
      return rawArray.isNull(index);
   }

   public boolean isEmpty() {
      return rawArray.isEmpty();
   }

   public int size() {
      return rawArray.size();
   }

   public JsonValue get(int i) {
      return JsonValue.wrap(rawArray.get(i));
   }

   @Override
   public Iterator<JsonValue> iterator() {
      return new Iterator<JsonValue>() {
         private Iterator<javax.json.JsonValue> rawIterator = rawArray.iterator();

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
}
