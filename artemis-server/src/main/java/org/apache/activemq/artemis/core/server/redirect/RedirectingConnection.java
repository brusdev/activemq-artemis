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

import javax.security.auth.Subject;

public class RedirectingConnection {
   private String sourceIP = null;
   private String user = null;
   private Subject subject = null;
   private String sniHostName = null;

   public String getSourceIP() {
      return sourceIP;
   }

   public RedirectingConnection setSourceIP(String sourceIP) {
      this.sourceIP = sourceIP;
      return this;
   }

   public String getUser() {
      return user;
   }

   public RedirectingConnection setUser(String user) {
      this.user = user;
      return this;
   }

   public Subject getSubject() {
      return subject;
   }

   public RedirectingConnection setSubject(Subject subject) {
      this.subject = subject;
      return this;
   }

   public String getSniHostName() {
      return sniHostName;
   }

   public RedirectingConnection setSniHostName(String sniHostName) {
      this.sniHostName = sniHostName;
      return this;
   }

   public String getValue(RedirectKeyType keyType) {
      switch (keyType) {
         case SNIHostname:
            return sniHostName;
         case SOURCE_IP:
            return sourceIP;
         case USER:
            return user;
         default:
            throw new IllegalStateException("Unexpected value: " + keyType);
      }
   }
}
