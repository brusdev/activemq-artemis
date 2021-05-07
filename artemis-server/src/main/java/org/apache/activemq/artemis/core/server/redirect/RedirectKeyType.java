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

package org.apache.activemq.artemis.core.server.redirect;

public enum RedirectKeyType {
   SNIHostname, SOURCE_IP, USER;

   public static final String validValues;

   static {
      StringBuffer stringBuffer = new StringBuffer();
      for (RedirectKeyType type : RedirectKeyType.values()) {

         if (stringBuffer.length() != 0) {
            stringBuffer.append(",");
         }

         stringBuffer.append(type.name());
      }

      validValues = stringBuffer.toString();
   }

   public static RedirectKeyType getType(String type) {
      switch (type) {
         case "SNIHostname":
            return SNIHostname;
         case "SOURCE_IP":
            return SOURCE_IP;
         case "USER":
            return USER;
         default:
            throw new IllegalStateException("Invalid RedirectKey:" + type + " valid Types: " + validValues);
      }
   }
}
