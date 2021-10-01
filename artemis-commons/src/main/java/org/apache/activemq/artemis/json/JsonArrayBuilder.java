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

public class JsonArrayBuilder {

   private final javax.json.JsonArrayBuilder rawArrayBuilder;

   public javax.json.JsonArrayBuilder getRawArrayBuilder() {
      return rawArrayBuilder;
   }

   public JsonArrayBuilder(javax.json.JsonArrayBuilder rawArrayBuilder) {
      this.rawArrayBuilder = rawArrayBuilder;
   }

   public JsonArrayBuilder add(JsonValue value) {
      rawArrayBuilder.add(value.getRawValue());
      return this;
   }

   public JsonArrayBuilder add(String value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(BigDecimal value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(BigInteger value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(int value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(long value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(double value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder add(boolean value) {
      rawArrayBuilder.add(value);
      return this;
   }

   public JsonArrayBuilder addNull() {
      rawArrayBuilder.addNull();
      return this;
   }

   public JsonArrayBuilder add(JsonObjectBuilder builder) {
      rawArrayBuilder.add(builder.getRawObjectBuilder());
      return this;
   }

   public JsonArrayBuilder add(JsonArrayBuilder builder) {
      rawArrayBuilder.add(builder.getRawArrayBuilder());
      return this;
   }

   public JsonArrayBuilder addAll(JsonArrayBuilder builder) {
      rawArrayBuilder.addAll(builder.getRawArrayBuilder());
      return this;
   }

   public JsonArrayBuilder add(int index, JsonValue value) {
      rawArrayBuilder.add(index, value.getRawValue());
      return this;
   }

   public JsonArrayBuilder add(int index, String value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, BigDecimal value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, BigInteger value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, int value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, long value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, double value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder add(int index, boolean value) {
      rawArrayBuilder.add(index, value);
      return this;
   }

   public JsonArrayBuilder addNull(int index) {
      rawArrayBuilder.addNull(index);
      return this;
   }

   public JsonArrayBuilder add(int index, JsonObjectBuilder builder) {
      rawArrayBuilder.add(index, builder.getRawObjectBuilder());
      return this;
   }

   public JsonArrayBuilder add(int index, JsonArrayBuilder builder) {
      rawArrayBuilder.add(index, builder.getRawArrayBuilder());
      return this;
   }

   public JsonArrayBuilder set(int index, JsonValue value) {
      rawArrayBuilder.set(index, value.getRawValue());
      return this;
   }

   public JsonArrayBuilder set(int index, String value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, BigDecimal value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, BigInteger value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, int value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, long value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, double value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder set(int index, boolean value) {
      rawArrayBuilder.set(index, value);
      return this;
   }

   public JsonArrayBuilder setNull(int index) {
      rawArrayBuilder.setNull(index);
      return this;
   }

   public JsonArrayBuilder set(int index, JsonObjectBuilder builder) {
      rawArrayBuilder.set(index, builder.getRawObjectBuilder());
      return this;
   }

   public JsonArrayBuilder set(int index, JsonArrayBuilder builder) {
      rawArrayBuilder.set(index, builder.getRawArrayBuilder());
      return this;
   }

   public JsonArrayBuilder remove(int index) {
      rawArrayBuilder.remove(index);
      return this;
   }

   public JsonArray build() {
      return new JsonArray(rawArrayBuilder.build());
   }
}
