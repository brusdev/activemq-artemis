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

import java.io.Serializable;

public class BrokerBalancerConfiguration implements Serializable {

   private String name = null;
   private int affinityTimeout = 0;
   private PoolConfiguration poolConfiguration = null;
   private PolicyConfiguration policyConfiguration = null;

   public String getName() {
      return name;
   }

   public BrokerBalancerConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public int getAffinityTimeout() {
      return affinityTimeout;
   }

   public BrokerBalancerConfiguration setAffinityTimeout(int affinityTimeout) {
      this.affinityTimeout = affinityTimeout;
      return this;
   }

   public PolicyConfiguration getPolicyConfiguration() {
      return policyConfiguration;
   }

   public BrokerBalancerConfiguration setPolicyConfiguration(PolicyConfiguration policyConfiguration) {
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
}
