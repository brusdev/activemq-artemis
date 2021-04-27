/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.redirection.RedirectAlgorithm;
import org.apache.activemq.artemis.core.server.redirection.RedirectKey;

public class RedirectConfiguration implements Serializable {

   private String name = null;
   private String sourceIP = null;
   private String user = null;
   private String userRole = null;
   private RedirectAlgorithm algorithm = RedirectAlgorithm.valueOf(ActiveMQDefaultConfiguration.getDefaultRedirectAlgorithm());
   private RedirectKey key = RedirectKey.valueOf(ActiveMQDefaultConfiguration.getDefaultRedirectKey());
   private List<String> staticConnectors = Collections.emptyList();
   private String discoveryGroupName = null;

   public String getName() {
      return name;
   }

   public RedirectConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public String getSourceIP() {
      return sourceIP;
   }

   public RedirectConfiguration setSourceIP(String sourceIP) {
      this.sourceIP = sourceIP;
      return this;
   }

   public String getUser() {
      return user;
   }

   public RedirectConfiguration setUser(String user) {
      this.user = user;
      return this;
   }

   public String getUserRole() {
      return userRole;
   }

   public RedirectConfiguration setUserRole(String userRole) {
      this.userRole = userRole;
      return this;
   }

   public RedirectAlgorithm getAlgorithm() {
      return algorithm;
   }

   public RedirectConfiguration setAlgorithm(RedirectAlgorithm algorithm) {
      this.algorithm = algorithm;
      return this;
   }

   public RedirectKey getKey() {
      return key;
   }

   public RedirectConfiguration setKey(RedirectKey key) {
      this.key = key;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   public RedirectConfiguration setStaticConnectors(List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   public RedirectConfiguration setDiscoveryGroupName(String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }
}
