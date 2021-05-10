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

import org.apache.activemq.artemis.spi.core.remoting.Connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedirectKeyBuilder {
   private RedirectKey key;
   private String username;
   private Connection connection;

   private static final Pattern ipv4Pattern = Pattern.compile("^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.(?!$)|$)){4}$");

   public String getUsername() {
      return username;
   }

   public RedirectKeyBuilder setUsername(String username) {
      this.username = username;
      return this;
   }

   public Connection getConnection() {
      return connection;
   }

   public RedirectKeyBuilder setConnection(Connection connection) {
      this.connection = connection;
      this.key = connection != null ? RedirectKey.valueOf(connection.getRedirectKey()) : null;
      return this;
   }

   public String build() {
      switch (key) {
         case SNI_HOST:
            return connection.getSNIHostName();
         case SOURCE_IP:
            if (connection.getRemoteAddress() != null) {
               Matcher ipv4Matcher = ipv4Pattern.matcher(connection.getRemoteAddress());

               if (ipv4Matcher.find()) {
                  return ipv4Matcher.group();
               }
            }
            return null;
         case USER_NAME:
            return username;
         default:
            throw new IllegalStateException("Unexpected value: " + key);
      }
   }
}
