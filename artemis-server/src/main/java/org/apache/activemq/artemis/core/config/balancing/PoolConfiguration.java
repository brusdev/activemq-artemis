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

package org.apache.activemq.artemis.core.config.balancing;

import java.util.Collections;
import java.util.List;

public class PoolConfiguration {
   private String username;

   private String password;

   private String localConnector = null;

   private List<String> staticConnectors = Collections.emptyList();

   private String discoveryGroupName = null;

   private int checkPeriod = 5000;

   private int quorumSize = 1;


   public String getUsername() {
      return username;
   }

   public PoolConfiguration setUsername(String username) {
      this.username = username;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public PoolConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   public int getCheckPeriod() {
      return checkPeriod;
   }

   public PoolConfiguration setCheckPeriod(int checkPeriod) {
      this.checkPeriod = checkPeriod;
      return this;
   }

   public int getQuorumSize() {
      return quorumSize;
   }

   public PoolConfiguration setQuorumSize(int quorumSize) {
      this.quorumSize = quorumSize;
      return this;
   }

   public String getLocalConnector() {
      return localConnector;
   }

   public PoolConfiguration setLocalConnector(String localConnector) {
      this.localConnector = localConnector;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   public PoolConfiguration setStaticConnectors(List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   public PoolConfiguration setDiscoveryGroupName(String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }
}
