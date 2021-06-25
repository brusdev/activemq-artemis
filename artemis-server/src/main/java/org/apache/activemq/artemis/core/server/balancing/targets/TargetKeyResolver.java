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

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.spi.core.remoting.Connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TargetKeyResolver {
   private static final Pattern ipv4Pattern = Pattern.compile("^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.(?!$)|$)){4}$");

   private final TargetKey key;

   public TargetKey getKey() {
      return key;
   }

   public TargetKeyResolver(TargetKey key) {
      this.key = key;
   }

   public String resolve(Connection connection, String clientID, String username) {
      String keyValue = null;

      switch (key) {
         case CLIENT_ID:
            keyValue = clientID;
            break;
         case SNI_HOST:
            keyValue = connection.getSNIHostName();
            break;
         case SOURCE_IP:
            if (connection.getRemoteAddress() != null) {
               Matcher ipv4Matcher = ipv4Pattern.matcher(connection.getRemoteAddress());

               if (ipv4Matcher.find()) {
                  keyValue = ipv4Matcher.group();
               }
            }
            break;
         case USER_NAME:
            keyValue = username;
            break;
         default:
            throw new IllegalStateException("Unexpected value: " + key);
      }

      if (keyValue == null) {
         keyValue = "DEFAULT";
      }

      return keyValue;
   }
}
