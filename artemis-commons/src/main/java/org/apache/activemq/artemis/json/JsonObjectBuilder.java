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

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonObjectBuilder {

   private final javax.json.JsonObjectBuilder rawObjectBuilder;

   public javax.json.JsonObjectBuilder getRawObjectBuilder() {
      return rawObjectBuilder;
   }

   public JsonObjectBuilder(javax.json.JsonObjectBuilder rawObjectBuilder) {
      this.rawObjectBuilder = rawObjectBuilder;
   }

   public JsonObjectBuilder add(String name, JsonValue value) {
      rawObjectBuilder.add(name, value.getRawValue());
      return this;
   }

   public JsonObjectBuilder add(String name, String value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, BigInteger value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, BigDecimal value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, int value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, long value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, double value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder add(String name, boolean value) {
      rawObjectBuilder.add(name, value);
      return this;
   }

   public JsonObjectBuilder addNull(String name) {
      rawObjectBuilder.addNull(name);
      return this;
   }

   public JsonObjectBuilder add(String name, JsonObjectBuilder builder) {
      rawObjectBuilder.add(name, builder.getRawObjectBuilder());
      return this;
   }

   public JsonObjectBuilder add(String name, JsonArrayBuilder builder) {
      rawObjectBuilder.add(name, builder.getRawArrayBuilder());
      return this;
   }

   public JsonObjectBuilder addAll(JsonObjectBuilder builder) {
      rawObjectBuilder.addAll(builder.getRawObjectBuilder());
      return this;
   }

   public JsonObjectBuilder remove(String name) {
      rawObjectBuilder.remove(name);
      return this;
   }

   public JsonObject build() {
      return new JsonObject(rawObjectBuilder.build());
   }
}
