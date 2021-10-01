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

public class JsonValue {

   public enum ValueType {
      /**
       * JSON array.
       */
      ARRAY,

      /**
       * JSON object.
       */
      OBJECT,

      /**
       * JSON string.
       */
      STRING,

      /**
       * JSON number.
       */
      NUMBER,

      /**
       * JSON true.
       */
      TRUE,

      /**
       * JSON false.
       */
      FALSE,

      /**
       * JSON null.
       */
      NULL
   }

   public static final JsonObject EMPTY_JSON_OBJECT = new JsonObject(javax.json.JsonValue.EMPTY_JSON_OBJECT);

   public static final JsonArray EMPTY_JSON_ARRAY = new JsonArray(javax.json.JsonValue.EMPTY_JSON_ARRAY);

   public static final JsonValue NULL = new JsonValue(javax.json.JsonValue.NULL);

   public static final JsonValue TRUE = new JsonValue(javax.json.JsonValue.TRUE);

   public static final JsonValue FALSE = new JsonValue(javax.json.JsonValue.FALSE);

   public static JsonValue wrap(javax.json.JsonValue rawValue) {
      if (rawValue == null) {
         return null;
      } else if (rawValue instanceof javax.json.JsonArray) {
         return new JsonArray((javax.json.JsonArray) rawValue);
      } else if (rawValue instanceof javax.json.JsonObject) {
         return new JsonObject((javax.json.JsonObject) rawValue);
      } else if (rawValue instanceof javax.json.JsonString) {
         return new JsonString((javax.json.JsonString) rawValue);
      } else if (rawValue instanceof javax.json.JsonNumber) {
         return new JsonNumber((javax.json.JsonNumber) rawValue);
      } else if (rawValue == javax.json.JsonValue.EMPTY_JSON_OBJECT) {
         return JsonValue.EMPTY_JSON_OBJECT;
      } else if (rawValue == javax.json.JsonValue.EMPTY_JSON_ARRAY) {
         return JsonValue.EMPTY_JSON_ARRAY;
      } else if (rawValue == javax.json.JsonValue.TRUE) {
         return JsonValue.TRUE;
      } else if (rawValue == javax.json.JsonValue.FALSE) {
         return JsonValue.FALSE;
      } else if (rawValue == javax.json.JsonValue.NULL) {
         return JsonValue.NULL;
      } else {
         return new JsonValue(rawValue);
      }
   }

   private final javax.json.JsonValue rawValue;

   public javax.json.JsonValue getRawValue() {
      return rawValue;
   }

   public JsonValue(javax.json.JsonValue rawValue) {
      this.rawValue = rawValue;
   }

   public ValueType getValueType() {
      return ValueType.valueOf(rawValue.getValueType().name());
   }

   public JsonObject asJsonObject() {
      return JsonObject.class.cast(this);
   }

   public JsonArray asJsonArray() {
      return JsonArray.class.cast(this);
   }

   @Override
   public String toString() {
      return rawValue.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (obj instanceof JsonValue) {
         return rawValue.equals(((JsonValue)obj).getRawValue());
      }
      return super.equals(obj);
   }
}
