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
package org.apache.activemq.artemis.core.config.balancing;

import org.apache.activemq.artemis.core.server.balancing.ConnectionKey;

import java.io.Serializable;

public class BrokerBalancerConfiguration implements Serializable {

   private String name = null;
   private ConnectionKey connectionKey = ConnectionKey.SOURCE_IP;
   private String connectionKeyFilter = null;
   private String localTargetFilter = null;
   private PoolConfiguration poolConfiguration = null;
   private BrokerServiceConfiguration cacheConfiguration = null;
   private BrokerServiceConfiguration policyConfiguration = null;
   private BrokerServiceConfiguration connectionKeyConfiguration = null;

   public String getName() {
      return name;
   }

   public BrokerBalancerConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public ConnectionKey getConnectionKey() {
      return connectionKey;
   }

   public BrokerBalancerConfiguration setConnectionKey(ConnectionKey connectionKey) {
      this.connectionKey = connectionKey;
      return this;
   }

   public String getConnectionKeyFilter() {
      return connectionKeyFilter;
   }

   public BrokerBalancerConfiguration setConnectionKeyFilter(String connectionKeyFilter) {
      this.connectionKeyFilter = connectionKeyFilter;
      return this;
   }

   public String getLocalTargetFilter() {
      return localTargetFilter;
   }

   public BrokerBalancerConfiguration setLocalTargetFilter(String localTargetFilter) {
      this.localTargetFilter = localTargetFilter;
      return this;
   }

   public BrokerServiceConfiguration getCacheConfiguration() {
      return cacheConfiguration;
   }

   public BrokerBalancerConfiguration setCacheConfiguration(BrokerServiceConfiguration cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
      return this;
   }

   public BrokerServiceConfiguration getPolicyConfiguration() {
      return policyConfiguration;
   }

   public BrokerBalancerConfiguration setPolicyConfiguration(BrokerServiceConfiguration policyConfiguration) {
      this.policyConfiguration = policyConfiguration;
      return this;
   }

   public PoolConfiguration getPoolConfiguration() {
      return poolConfiguration;
   }

   public BrokerBalancerConfiguration setPoolConfiguration(PoolConfiguration poolConfiguration) {
      this.poolConfiguration = poolConfiguration;
      return this;
   }

   public void setConnectionKeyConfiguration(BrokerServiceConfiguration configuration) {
      this.connectionKeyConfiguration = configuration;
   }

   public BrokerServiceConfiguration getConnectionKeyConfiguration() {
      return connectionKeyConfiguration;
   }
}
