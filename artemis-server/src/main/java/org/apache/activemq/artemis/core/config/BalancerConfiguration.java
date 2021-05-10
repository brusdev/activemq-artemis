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

public class BalancerConfiguration implements Serializable {

   private String name = null;
   private int affinityTimeout;
   private String policyName = null;
   private List<String> staticConnectors = Collections.emptyList();
   private String discoveryGroupName = null;

   public String getName() {
      return name;
   }

   public BalancerConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public int getAffinityTimeout() {
      return affinityTimeout;
   }

   public BalancerConfiguration setAffinityTimeout(int affinityTimeout) {
      this.affinityTimeout = affinityTimeout;
      return this;
   }

   public String getPolicyName() {
      return policyName;
   }

   public BalancerConfiguration setPolicyName(String policyName) {
      this.policyName = policyName;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   public BalancerConfiguration setStaticConnectors(List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   public BalancerConfiguration setDiscoveryGroupName(String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }
}
