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

public class JsonNumber extends JsonValue {

   private final javax.json.JsonNumber rawNumber;

   public javax.json.JsonNumber getRawNumber() {
      return rawNumber;
   }

   public JsonNumber(javax.json.JsonNumber rawNumber) {
      super(rawNumber);
      this.rawNumber = rawNumber;
   }

   public boolean isIntegral() {
      return rawNumber.isIntegral();
   }

   public int intValue() {
      return rawNumber.intValue();
   }

   public int intValueExact() {
      return rawNumber.intValueExact();
   }

   public long longValue() {
      return rawNumber.longValue();
   }

   public long longValueExact() {
      return rawNumber.longValueExact();
   }

   public BigInteger bigIntegerValue() {
      return rawNumber.bigIntegerValue();
   }

   public BigInteger bigIntegerValueExact() {
      return rawNumber.bigIntegerValueExact();
   }

   public double doubleValue() {
      return rawNumber.doubleValue();
   }

   public BigDecimal bigDecimalValue() {
      return rawNumber.bigDecimalValue();
   }

   public Number numberValue() {
      return rawNumber.numberValue();
   }
}
