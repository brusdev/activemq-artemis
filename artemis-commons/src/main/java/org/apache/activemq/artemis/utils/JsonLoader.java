/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils;

import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonArrayBuilder;
import org.apache.activemq.artemis.json.JsonNumber;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.json.JsonString;

import javax.json.JsonReader;
import javax.json.spi.JsonProvider;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This is to make sure we use the proper classLoader to load JSon libraries.
 * This is equivalent to using {@link javax.json.Json}
 */
public class JsonLoader {

   private static final JsonProvider provider;

   static {
      provider = loadProvider();
   }

   private static JsonProvider loadProvider() {
      return AccessController.doPrivileged(new PrivilegedAction<JsonProvider>() {
         @Override
         public JsonProvider run() {
            ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
            try {
               Thread.currentThread().setContextClassLoader(JsonLoader.class.getClassLoader());
               return JsonProvider.provider();
            } finally {
               Thread.currentThread().setContextClassLoader(originalLoader);
            }
         }
      });

   }

   public static JsonObject readObject(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return new JsonObject(jsonReader.readObject());
      }
   }

   public static JsonArray readArray(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return new JsonArray(jsonReader.readArray());
      }
   }

   public static JsonArrayBuilder createArrayBuilder() {
      return new JsonArrayBuilder(provider.createArrayBuilder());
   }

   public static JsonObjectBuilder createObjectBuilder() {
      return new JsonObjectBuilder(provider.createObjectBuilder());
   }

   public static JsonString createValue(String value) {
      return new JsonString(provider.createValue(value));
   }

   public static JsonNumber createValue(int value) {
      return new JsonNumber(provider.createValue(value));
   }

   public static JsonNumber createValue(long value) {
      return new JsonNumber(provider.createValue(value));
   }

   public static JsonNumber createValue(double value) {
      return new JsonNumber(provider.createValue(value));
   }

   public static JsonNumber createValue(BigDecimal value) {
      return new JsonNumber(provider.createValue(value));
   }

   public static JsonNumber createValue(BigInteger value) {
      return new JsonNumber(provider.createValue(value));
   }
}
